"""Bounded capacity-sample history operations."""

from __future__ import annotations

import sqlite3

from agent_run.errors import ValidationError

from .db import immediate, nonblank


import json
import math
from collections.abc import Iterable, Mapping
from typing import cast

from .db import insert_capacity_row, timestamp

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


def _sample_values(sample: Mapping[str, object], runtime: str) -> dict[str, object]:
    """Validate and normalize one sample after the public type boundary.

    ``sample`` is a mapping containing nonblank ``lane``, ``window``, and
    ``source`` strings, with an optional nonblank ``runtime`` that must equal
    the batch ``runtime``. Numeric percentages must be finite and in the
    inclusive 0--100 range, timestamps must be finite and nonnegative, and
    nested payloads must satisfy the UTF-8 JSON bound. The returned dictionary
    owns normalized values for insertion; invalid input raises
    :class:`ValidationError` and performs no I/O.
    """

    values: dict[str, object] = {"runtime": sample.get("runtime", runtime)}
    for name in ("runtime", "lane", "window", "source"):
        value = values.get(name, sample.get(name))
        nonblank(name, cast(str, value))
        values[name] = value
    if values["runtime"] != runtime:
        raise ValidationError("capacity sample runtime must match batch runtime")
    remaining = sample.get("remaining_percent")
    if remaining is not None and (
        isinstance(remaining, bool)
        or not isinstance(remaining, (int, float))
        or not math.isfinite(remaining)
        or not 0 <= remaining <= 100
    ):
        raise ValidationError("remaining_percent must be between 0 and 100")
    values.update(
        lane=cast(str, sample["lane"]), window=cast(str, sample["window"]),
        source=cast(str, sample["source"]), target=cast(str | None, sample.get("target")),
        remaining_percent=cast(float | None, remaining),
        reset_at=None if sample.get("reset_at") is None else timestamp(cast(float, sample["reset_at"])),
        observed_at=None if sample.get("observed_at") is None else timestamp(cast(float, sample["observed_at"])),
        valid_until=None if sample.get("valid_until") is None else timestamp(cast(float, sample["valid_until"])),
        payload_json=_route_payload(sample.get("payload")),
    )
    return values


def append_capacity_samples(
    connection: sqlite3.Connection,
    samples: Iterable[Mapping[str, object]],
    *,
    runtime: str,
    scope_id: str,
    observed_at: float,
    valid_until: float,
    payload: object,
) -> None:
    """Atomically append samples and upsert one bounded route snapshot.

    ``connection`` is the owning SQLite connection; ``samples`` is consumed
    once and every item must be a mapping for ``runtime``. ``scope_id`` is a
    nonblank snapshot key, while ``observed_at`` and ``valid_until`` are
    finite nonnegative timestamps with expiry no earlier than observation.
    ``payload`` must be JSON serializable and at most 65,536 UTF-8 bytes. All
    validation occurs before the immediate transaction, and any insertion or
    upsert failure rolls back every sample and the snapshot together.
    """

    nonblank("runtime", runtime)
    nonblank("scope_id", scope_id)
    observed = timestamp(observed_at)
    expiry = timestamp(valid_until)
    if expiry < observed:
        raise ValidationError("valid_until must be greater than or equal to observed_at")
    payload_json = _route_payload(payload)
    validated: list[dict[str, object]] = []
    for sample in samples:
        if not isinstance(sample, Mapping):
            raise ValidationError("capacity samples must be mappings")
        validated.append(_sample_values(sample, runtime))
    with immediate(connection):
        for values in validated:
            insert_capacity_row(
                connection,
                runtime=cast(str, values["runtime"]),
                lane=cast(str, values["lane"]),
                window=cast(str, values["window"]),
                target=cast(str | None, values["target"]),
                source=cast(str, values["source"]),
                remaining_percent=cast(float | None, values["remaining_percent"]),
                reset_at=cast(float | None, values["reset_at"]),
                observed_at=cast(float | None, values["observed_at"]),
                valid_until=cast(float | None, values["valid_until"]),
                payload_json=cast(str, values["payload_json"]),
            )
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
