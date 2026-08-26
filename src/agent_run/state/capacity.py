"""Bounded capacity-sample history operations."""

from __future__ import annotations

import sqlite3

from agent_run.errors import ValidationError

from .db import immediate, nonblank


def _retention(value: int) -> int:
    if type(value) is not int or value < 1:
        raise ValidationError("retention must be a positive integer")
    return value


def prune_capacity_samples(connection: sqlite3.Connection, retention: int) -> int:
    retention = _retention(retention)
    with immediate(connection):
        connection.execute(
            """DELETE FROM capacity_samples WHERE id IN (
                   SELECT id FROM (
                       SELECT id, ROW_NUMBER() OVER (
                           ORDER BY observed_at DESC, id DESC
                       ) AS position
                       FROM capacity_samples
                   ) WHERE position > ?
               )""",
            (retention,),
        )
        return int(connection.execute("SELECT changes()").fetchone()[0])


def capacity_sample_history(
    connection: sqlite3.Connection,
    *,
    retention: int,
    runtime: str | None = None,
) -> list[sqlite3.Row]:
    retention = _retention(retention)
    if runtime is not None:
        nonblank("runtime", runtime)
    rows = connection.execute(
        """SELECT id, runtime, lane, window, target, source,
                  remaining_percent, reset_at, observed_at, valid_until, payload_json
           FROM capacity_samples
           WHERE (? IS NULL OR runtime = ?)
             AND id IN (
                 SELECT id FROM capacity_samples
                 ORDER BY observed_at DESC, id DESC LIMIT ?
             )
           ORDER BY observed_at DESC, id DESC""",
        (runtime, runtime, retention),
    )
    return list(rows)
