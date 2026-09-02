"""Read-only, bounded state snapshot for diagnostics."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path

from agent_run.domain import ACTIVE
from agent_run.errors import ValidationError

from .db import _validate_schema, integer, timestamp


@dataclass(frozen=True, slots=True)
class DiagnosticSnapshot:
    agents: tuple[dict[str, object], ...]
    capacity: tuple[dict[str, object], ...]


def diagnostic_snapshot(
    database: str | Path, *, at: float, limit: int = 256
) -> DiagnosticSnapshot:
    """Read bounded active agents and the newest row per capacity identity.

    ``limit`` caps agents and distinct capacity identities independently.
    Repeated healthy samples must not hide an older, stale sibling identity.
    The database is opened read-only; invalid paths, schema or bounds raise
    ``ValidationError`` or the existing schema error without modifying state.
    """
    path = Path(database).expanduser().resolve()
    if not path.is_file():
        raise ValidationError(f"state database does not exist: {path}")
    integer("limit", limit, minimum=1)
    if limit > 1_000:
        raise ValidationError("limit must not exceed 1000")
    timestamp(at)
    connection = sqlite3.connect(
        f"{path.as_uri()}?mode=ro", uri=True, isolation_level=None, timeout=1.0
    )
    connection.row_factory = sqlite3.Row
    try:
        connection.execute("PRAGMA query_only=ON")
        _validate_schema(connection)
        statuses = tuple(sorted(status.value for status in ACTIVE))
        placeholders = ",".join("?" for _ in statuses)
        agents = connection.execute(
            f"""SELECT * FROM agents WHERE status IN ({placeholders})
                ORDER BY created_at DESC, id DESC LIMIT ?""",
            (*statuses, limit),
        )
        capacity = connection.execute(
            """SELECT id, runtime, lane, window, target, source,
                      observed_at, valid_until
               FROM (
                   SELECT *, ROW_NUMBER() OVER (
                       PARTITION BY runtime, lane, window, target, source
                       ORDER BY observed_at DESC, id DESC
                   ) AS position
                   FROM capacity_samples
               ) WHERE position = 1
               ORDER BY observed_at DESC, id DESC LIMIT ?""",
            (limit,),
        )
        return DiagnosticSnapshot(
            tuple(dict(row) for row in agents),
            tuple(dict(row) for row in capacity),
        )
    finally:
        connection.close()
