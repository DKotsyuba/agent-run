"""Delivery outbox operations shared by the state service."""

from __future__ import annotations

import sqlite3

from agent_run.errors import ValidationError

from .db import (
    claim_delivery_row,
    finish_delivery_claim,
    immediate,
    nonblank,
    owned_delivery_attempts,
    positive_number,
    row_dict,
    timestamp,
)

_MAX_EVIDENCE_JSON_BYTES = 16384


def _owned_attempt(
    connection: sqlite3.Connection,
    delivery_id: str,
    owner: str,
    now: float,
    evidence_json: str | None,
) -> int:
    """Verify one live claim and optionally persist its immutable evidence."""

    attempts = owned_delivery_attempts(connection, delivery_id, owner, now)
    if attempts is None:
        raise ValidationError("delivery lease is not owned by caller")
    if evidence_json is not None:
        nonblank("delivery attempt evidence", evidence_json)
        if len(evidence_json.encode("utf-8")) > _MAX_EVIDENCE_JSON_BYTES:
            raise ValidationError("delivery attempt evidence exceeds 16384 bytes")
        connection.execute(
            """INSERT INTO delivery_attempt_evidence
               (delivery_id, attempt, recorded_at, evidence_json)
               VALUES (?, ?, ?, ?)""",
            (delivery_id, attempts, now, evidence_json),
        )
    return attempts


def claim_delivery(
    connection: sqlite3.Connection,
    owner: str,
    *,
    at: float | None = None,
    lease_seconds: float = 30,
) -> dict[str, object] | None:
    nonblank("lease owner", owner)
    now = timestamp(at)
    lease_seconds = positive_number("lease_seconds", lease_seconds)
    with immediate(connection):
        row = claim_delivery_row(connection, owner, now, now + lease_seconds)
    return row_dict(row)


def complete_delivery(
    connection: sqlite3.Connection,
    delivery_id: str,
    owner: str,
    *,
    remote_message_id: str | None = None,
    ambiguous_result: bool = False,
    evidence_json: str | None = None,
    at: float | None = None,
) -> None:
    nonblank("delivery_id", delivery_id)
    nonblank("lease owner", owner)
    now = timestamp(at)
    with immediate(connection):
        _owned_attempt(connection, delivery_id, owner, now, evidence_json)
        if not finish_delivery_claim(
            connection,
            delivery_id,
            owner,
            "delivered",
            now=now,
            remote_message_id=remote_message_id,
            ambiguous_result=ambiguous_result,
        ):
            raise ValidationError("delivery lease is not owned by caller")


def fail_delivery(
    connection: sqlite3.Connection,
    delivery_id: str,
    owner: str,
    error: str,
    *,
    at: float | None = None,
    ambiguous_result: bool = False,
    evidence_json: str | None = None,
) -> None:
    nonblank("delivery_id", delivery_id)
    nonblank("lease owner", owner)
    nonblank("delivery error", error)
    now = timestamp(at)
    with immediate(connection):
        _owned_attempt(connection, delivery_id, owner, now, evidence_json)
        if not finish_delivery_claim(
            connection,
            delivery_id,
            owner,
            "failed",
            now=now,
            last_error=error,
            ambiguous_result=ambiguous_result,
        ):
            raise ValidationError("delivery lease is not owned by caller")


def retry_delivery(
    connection: sqlite3.Connection,
    delivery_id: str,
    owner: str,
    error: str,
    *,
    at: float | None = None,
    ambiguous_result: bool = False,
    evidence_json: str | None = None,
    base_delay: float = 1,
    max_delay: float = 300,
) -> float:
    nonblank("delivery_id", delivery_id)
    nonblank("lease owner", owner)
    nonblank("delivery error", error)
    base_delay = positive_number("base_delay", base_delay)
    max_delay = positive_number("max_delay", max_delay)
    now = timestamp(at)
    with immediate(connection):
        attempts = _owned_attempt(
            connection, delivery_id, owner, now, evidence_json
        )
        delay = min(max_delay, base_delay * (2 ** min(attempts - 1, 20)))
        next_attempt_at = now + delay
        finish_delivery_claim(
            connection,
            delivery_id,
            owner,
            "retry_wait",
            now=now,
            last_error=error,
            ambiguous_result=ambiguous_result,
            next_attempt_at=next_attempt_at,
        )
    return next_attempt_at


def cancel_delivery(connection: sqlite3.Connection, delivery_id: str) -> bool:
    nonblank("delivery_id", delivery_id)
    with immediate(connection):
        updated = connection.execute(
            """UPDATE deliveries SET state = 'cancelled', lease_owner = NULL,
               lease_until = NULL, next_attempt_at = NULL
               WHERE id = ? AND state NOT IN ('delivered', 'cancelled')""",
            (delivery_id,),
        ).rowcount
    return updated == 1


#: Age in seconds a completion delivery keeps waiting for an orchestrator
#: session to bind to before the dispatcher gives up on it.
BINDING_WINDOW_SECONDS = 3600.0


def expire_unbound_deliveries(
    connection: sqlite3.Connection,
    *,
    at: float | None = None,
    max_age_seconds: float = BINDING_WINDOW_SECONDS,
) -> list[str]:
    """Move completion deliveries that can no longer bind to the ``expired`` state.

    A delivery is expirable only when all three guards hold: it is still
    ``waiting_binding`` (no orchestrator session was ever attached), the agent it
    belongs to is already terminal -- so the bind hook that would have attached a
    session can no longer fire -- and the terminal event that created the row is
    older than ``max_age_seconds``.  A live agent's delivery is never expired,
    and a recently finished one keeps its full window to bind.

    Runs as one immediate transaction.  Every expirable row is set to
    ``expired`` with its lease and schedule cleared, which leaves it invisible to
    the claim scan and therefore permanently undispatched.

    Returns the ids that were expired, oldest first, so the caller can log one
    line per delivery rather than one per scan.

    Raises :class:`ValidationError` when ``max_age_seconds`` is not a positive
    finite number.
    """

    max_age_seconds = positive_number("max_age_seconds", max_age_seconds)
    now = timestamp(at)
    with immediate(connection):
        rows = connection.execute(
            """SELECT d.id FROM deliveries d
               JOIN agents a ON a.id = d.agent_id
               JOIN events e ON e.seq = d.terminal_event_seq
               WHERE d.state = 'waiting_binding'
                 AND a.status IN
                     ('succeeded', 'failed', 'timed_out', 'cancelled', 'lost')
                 AND e.at <= ?
               ORDER BY e.at, d.id""",
            (now - max_age_seconds,),
        ).fetchall()
        expired = [str(row["id"]) for row in rows]
        for delivery_id in expired:
            connection.execute(
                """UPDATE deliveries SET state = 'expired', lease_owner = NULL,
                   lease_until = NULL, next_attempt_at = NULL
                   WHERE id = ? AND state = 'waiting_binding'""",
                (delivery_id,),
            )
    return expired
