"""Current capacity route-snapshot operations."""

from __future__ import annotations

import sqlite3

from agent_run.errors import ValidationError

from .db import immediate, nonblank


import json
from .db import timestamp

_MAX_ROUTE_PAYLOAD_BYTES = 65536


def _route_payload(value: object) -> str:
    """Serialize one topology payload into canonical bounded JSON.

    ``value`` is any JSON-compatible object; non-serializable values and
    encodings exceeding 65,536 UTF-8 bytes raise :class:`ValidationError`.
    The returned string is deterministic for equivalent mappings and has no
    side effects.
    """

    try:
        encoded = json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
    except (TypeError, ValueError) as error:
        raise ValidationError("route payload must be JSON serializable") from error
    if len(encoded.encode("utf-8")) > _MAX_ROUTE_PAYLOAD_BYTES:
        raise ValidationError("route payload exceeds 65536 UTF-8 bytes")
    return encoded


def replace_capacity_snapshot(
    connection: sqlite3.Connection,
    *,
    runtime: str,
    scope_id: str,
    observed_at: float,
    valid_until: float,
    payload: object,
) -> None:
    """Atomically replace one bounded current route snapshot.

    ``connection`` is the owning SQLite connection. ``scope_id`` is a
    nonblank snapshot key, while ``observed_at`` and ``valid_until`` are
    finite nonnegative timestamps with expiry no earlier than observation.
    ``payload`` must be JSON serializable and at most 65,536 UTF-8 bytes. All
    validation occurs before the immediate transaction.
    """

    nonblank("runtime", runtime)
    nonblank("scope_id", scope_id)
    observed = timestamp(observed_at)
    expiry = timestamp(valid_until)
    if expiry < observed:
        raise ValidationError("valid_until must be greater than or equal to observed_at")
    payload_json = _route_payload(payload)
    with immediate(connection):
        connection.execute(
            """INSERT INTO capacity_route_snapshots
               (runtime, scope_id, observed_at, valid_until, payload_json)
               VALUES (?, ?, ?, ?, ?)
               ON CONFLICT(runtime, scope_id) DO UPDATE SET
                 observed_at = excluded.observed_at,
                 valid_until = excluded.valid_until,
                 payload_json = excluded.payload_json""",
            (runtime, scope_id, observed, expiry, payload_json),
        )


def capacity_route_snapshots(
    connection: sqlite3.Connection, *, runtime: str | None = None
) -> list[sqlite3.Row]:
    """Return snapshots ordered deterministically by runtime and scope.

    ``connection`` is read without mutation. An omitted ``runtime`` returns
    every snapshot; otherwise only snapshots for the nonblank runtime are
    returned. The result is a newly allocated list of SQLite rows ordered by
    ``(runtime, scope_id)``.
    """

    params: tuple[object, ...] = () if runtime is None else (runtime,)
    runtime_filter = "" if runtime is None else "WHERE runtime = ?"
    return list(connection.execute(
        f"SELECT * FROM capacity_route_snapshots {runtime_filter} ORDER BY runtime, scope_id",
        params,
    ))
