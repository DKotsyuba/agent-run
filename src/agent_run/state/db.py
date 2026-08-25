"""SQLite connection and schema-version handling."""

from __future__ import annotations

import json
import math
import sqlite3
import time
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

from agent_run.domain import AgentId, OrchestratorRef, StartRequest
from agent_run.errors import ValidationError


SCHEMA_VERSION = 1
_SCHEMA_PATH = Path(__file__).with_name("schema.sql")
_TABLES = frozenset(
    {
        "orchestrator_sessions",
        "agents",
        "attempts",
        "events",
        "messages",
        "commands",
        "deliveries",
        "capacity_samples",
        "context_receipts",
    }
)


def timestamp(value: float | None = None) -> float:
    result = time.time() if value is None else value
    if isinstance(result, bool) or not isinstance(result, (int, float)):
        raise ValidationError("timestamp must be finite and nonnegative")
    result = float(result)
    if result < 0 or not math.isfinite(result):
        raise ValidationError("timestamp must be finite and nonnegative")
    return result


def nonblank(name: str, value: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValidationError(f"{name} must be a nonblank string")
    return value


def integer(name: str, value: int, *, minimum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise ValidationError(f"{name} must be an integer of at least {minimum}")
    return value


def positive_number(name: str, value: float) -> float:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or value <= 0
        or not math.isfinite(value)
    ):
        raise ValidationError(f"{name} must be positive and finite")
    return float(value)


def json_text(value: object) -> str:
    try:
        return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
    except (TypeError, ValueError) as error:
        raise ValidationError("value must be JSON serializable") from error


def request_json(request: StartRequest) -> str:
    orchestrator = request.orchestrator
    return json_text(
        {
            "runtime": request.runtime,
            "model": request.model,
            "profile": request.profile,
            "task": request.task,
            "workdir": str(request.workdir),
            "write": request.write,
            "effort": request.effort,
            "timeout_seconds": request.timeout_seconds,
            "read_roots": [str(path) for path in request.read_roots],
            "output_schema": request.output_schema,
            "orchestrator": None
            if orchestrator is None
            else {
                "transport": orchestrator.transport,
                "external_session_id": orchestrator.external_session_id,
                "external_turn_id": orchestrator.external_turn_id,
            },
            "request_id": request.request_id,
        }
    )


def row_dict(row: sqlite3.Row | None) -> dict[str, object] | None:
    return None if row is None else dict(row)


def agent_row(
    connection: sqlite3.Connection, agent_id: str | AgentId
) -> sqlite3.Row:
    row = connection.execute("SELECT * FROM agents WHERE id = ?", (agent_id,)).fetchone()
    if row is None:
        raise ValidationError(f"unknown agent: {agent_id}")
    return row


def idempotent_agent(
    connection: sqlite3.Connection, session_id: str, request_id: str
) -> sqlite3.Row | None:
    return connection.execute(
        """SELECT id, request_json, task_summary, config_revision FROM agents
           WHERE orchestrator_session_id = ? AND request_id = ?""",
        (session_id, request_id),
    ).fetchone()


def require_attempt(
    connection: sqlite3.Connection, agent_id: AgentId, attempt_id: str | None
) -> None:
    if attempt_id is None:
        return
    row = connection.execute(
        "SELECT 1 FROM attempts WHERE id = ? AND agent_id = ?", (attempt_id, agent_id)
    ).fetchone()
    if row is None:
        raise ValidationError(f"attempt does not belong to agent: {attempt_id}")


def session_for_ref(
    connection: sqlite3.Connection, ref: OrchestratorRef, at: float
) -> str:
    if not isinstance(ref, OrchestratorRef):
        raise ValidationError("orchestrator must be an OrchestratorRef")
    row = connection.execute(
        """SELECT id, external_turn_id FROM orchestrator_sessions
           WHERE transport = ? AND external_session_id = ?""",
        (ref.transport, ref.external_session_id),
    ).fetchone()
    if row is not None:
        if row["external_turn_id"] != ref.external_turn_id:
            raise ValidationError("orchestrator session target is immutable")
        connection.execute(
            "UPDATE orchestrator_sessions SET last_seen_at = MAX(last_seen_at, ?) WHERE id = ?",
            (at, row["id"]),
        )
        return str(row["id"])
    session_id = f"ors_{uuid.uuid4().hex}"
    connection.execute(
        """INSERT INTO orchestrator_sessions
           (id, transport, external_session_id, external_turn_id, created_at, last_seen_at)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (
            session_id,
            ref.transport,
            ref.external_session_id,
            ref.external_turn_id,
            at,
            at,
        ),
    )
    return session_id


def insert_event(
    connection: sqlite3.Connection,
    agent_id: AgentId,
    at: float,
    kind: str,
    *,
    attempt_id: str | None = None,
    from_status: str | None = None,
    to_status: str | None = None,
    data: object = None,
) -> int:
    cursor = connection.execute(
        """INSERT INTO events
           (agent_id, attempt_id, at, kind, from_status, to_status, data_json)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (
            agent_id,
            attempt_id,
            at,
            kind,
            from_status,
            to_status,
            json_text({} if data is None else data),
        ),
    )
    return int(cursor.lastrowid)


def insert_agent_row(
    connection: sqlite3.Connection,
    agent_id: AgentId,
    request: StartRequest,
    session_id: str | None,
    task_summary: str,
    serialized_request: str,
    config_revision: str,
    created_at: float,
) -> None:
    connection.execute(
        """INSERT INTO agents
           (id, request_id, orchestrator_session_id, runtime, model, profile,
            task, task_summary, workdir, request_json, status, created_at,
            timeout_seconds, config_revision)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'created', ?, ?, ?)""",
        (
            agent_id,
            request.request_id,
            session_id,
            request.runtime,
            request.model,
            request.profile,
            request.task,
            task_summary,
            str(request.workdir),
            serialized_request,
            created_at,
            request.timeout_seconds,
            config_revision,
        ),
    )


def insert_capacity_row(
    connection: sqlite3.Connection,
    *,
    runtime: str,
    lane: str,
    window: str,
    target: str | None,
    source: str,
    remaining_percent: float | None,
    reset_at: float | None,
    observed_at: float | None,
    valid_until: float | None,
    payload_json: str,
) -> int:
    cursor = connection.execute(
        """INSERT INTO capacity_samples
           (runtime, lane, window, target, source, remaining_percent,
            reset_at, observed_at, valid_until, payload_json)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            runtime,
            lane,
            window,
            target,
            source,
            remaining_percent,
            reset_at,
            observed_at,
            valid_until,
            payload_json,
        ),
    )
    return int(cursor.lastrowid)


def recent_capacity_rows(
    connection: sqlite3.Connection,
    now: float,
    runtime: str | None,
    limit: int,
) -> list[sqlite3.Row]:
    params: list[object] = [now]
    runtime_filter = ""
    if runtime is not None:
        runtime_filter = "AND runtime = ?"
        params.append(runtime)
    params.append(limit)
    return list(
        connection.execute(
            f"""SELECT * FROM capacity_samples
                WHERE (valid_until IS NULL OR valid_until >= ?) {runtime_filter}
                ORDER BY observed_at DESC, id DESC LIMIT ?""",
            params,
        )
    )


def count_agents(connection: sqlite3.Connection, statuses: tuple[str, ...]) -> int:
    row = connection.execute(
        f"SELECT COUNT(*) FROM agents WHERE status IN ({','.join('?' for _ in statuses)})",
        statuses,
    ).fetchone()
    return int(row[0])


def message_rows(
    connection: sqlite3.Connection, agent_id: AgentId, after_seq: int, limit: int
) -> list[sqlite3.Row]:
    return list(
        connection.execute(
            """SELECT * FROM messages
               WHERE agent_id = ? AND seq > ? ORDER BY seq LIMIT ?""",
            (agent_id, after_seq, limit),
        )
    )


def claim_delivery_row(
    connection: sqlite3.Connection, owner: str, now: float, lease_until: float
) -> sqlite3.Row | None:
    row = connection.execute(
        """SELECT id FROM deliveries
           WHERE ((state IN ('pending', 'retry_wait')
                   AND COALESCE(next_attempt_at, 0) <= ?)
                  OR (state = 'sending' AND lease_until <= ?))
           ORDER BY COALESCE(next_attempt_at, lease_until, 0), id LIMIT 1""",
        (now, now),
    ).fetchone()
    if row is None:
        return None
    updated = connection.execute(
        """UPDATE deliveries
           SET state = 'sending', attempts = attempts + 1,
               lease_owner = ?, lease_until = ?, next_attempt_at = NULL
           WHERE id = ? AND ((state IN ('pending', 'retry_wait')
                              AND COALESCE(next_attempt_at, 0) <= ?)
                             OR (state = 'sending' AND lease_until <= ?))""",
        (owner, lease_until, row["id"], now, now),
    ).rowcount
    if updated != 1:
        return None
    return connection.execute(
        """SELECT d.*, a.status AS agent_status, s.transport,
                  s.external_session_id, s.external_turn_id FROM deliveries d
           JOIN agents a ON a.id = d.agent_id
           JOIN orchestrator_sessions s ON s.id = d.orchestrator_session_id
           WHERE d.id = ?""",
        (row["id"],),
    ).fetchone()


def owned_delivery_attempts(
    connection: sqlite3.Connection, delivery_id: str, owner: str, now: float
) -> int | None:
    row = connection.execute(
        """SELECT attempts FROM deliveries
           WHERE id = ? AND state = 'sending' AND lease_owner = ? AND lease_until > ?""",
        (delivery_id, owner, now),
    ).fetchone()
    return None if row is None else int(row["attempts"])


def finish_delivery_claim(
    connection: sqlite3.Connection,
    delivery_id: str,
    owner: str,
    state: str,
    *,
    now: float,
    remote_message_id: str | None = None,
    last_error: str | None = None,
    ambiguous_result: bool | None = None,
    next_attempt_at: float | None = None,
) -> bool:
    updated = connection.execute(
        """UPDATE deliveries
           SET state = ?, remote_message_id = COALESCE(?, remote_message_id),
               last_error = COALESCE(?, last_error),
               ambiguous_result = MAX(ambiguous_result, COALESCE(?, 0)),
               next_attempt_at = ?, lease_owner = NULL, lease_until = NULL
           WHERE id = ? AND state = 'sending' AND lease_owner = ? AND lease_until > ?""",
        (
            state,
            remote_message_id,
            last_error,
            None if ambiguous_result is None else int(ambiguous_result),
            next_attempt_at,
            delivery_id,
            owner,
            now,
        ),
    ).rowcount
    return updated == 1


def _raw_connect(path: Path, *, existing: bool = False) -> sqlite3.Connection:
    target = f"{path.as_uri()}?mode=rw" if existing else path
    connection = sqlite3.connect(
        target,
        timeout=5.0,
        isolation_level=None,
        uri=existing,
    )
    connection.row_factory = sqlite3.Row
    return connection


def _version(connection: sqlite3.Connection) -> int:
    return int(connection.execute("PRAGMA user_version").fetchone()[0])


def _tables(connection: sqlite3.Connection) -> frozenset[str]:
    rows = connection.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'"
    )
    return frozenset(row[0] for row in rows)


def _validate_schema(connection: sqlite3.Connection) -> None:
    version = _version(connection)
    tables = _tables(connection)
    if version != SCHEMA_VERSION or not _TABLES.issubset(tables):
        raise ValidationError(
            f"unsupported or incomplete state schema: version {version}; "
            f"expected {SCHEMA_VERSION}"
        )


def _configure(connection: sqlite3.Connection) -> None:
    mode = connection.execute("PRAGMA journal_mode=WAL").fetchone()[0]
    if str(mode).lower() != "wal":
        raise ValidationError(f"state database could not enable WAL mode: {mode}")
    connection.execute("PRAGMA synchronous=FULL")
    connection.execute("PRAGMA foreign_keys=ON")
    connection.execute("PRAGMA busy_timeout=5000")


def _private_path(path: Path, *, parent_created: bool) -> None:
    if parent_created:
        path.parent.chmod(0o700)
    path.chmod(0o600)


def initialize_database(database: str | Path) -> sqlite3.Connection:
    """Create schema v1 or open an already-valid v1 database."""

    path = Path(database).expanduser().resolve()
    parent_created = not path.parent.exists()
    path.parent.mkdir(parents=True, exist_ok=True)
    existed = path.exists()
    connection = _raw_connect(path)
    try:
        version = _version(connection)
        tables = _tables(connection)
        if version == 0 and not tables:
            script = _SCHEMA_PATH.read_text(encoding="utf-8")
            try:
                connection.executescript(f"BEGIN IMMEDIATE;\n{script}\nCOMMIT;")
            except BaseException:
                connection.rollback()
                raise
        else:
            _validate_schema(connection)
        _validate_schema(connection)
        _configure(connection)
        _private_path(path, parent_created=parent_created)
        return connection
    except BaseException:
        connection.close()
        if not existed and path.exists() and path.stat().st_size == 0:
            path.unlink()
        raise


def open_database(database: str | Path) -> sqlite3.Connection:
    """Open an existing schema-v1 database without migrating it."""

    path = Path(database).expanduser().resolve()
    if not path.is_file():
        raise ValidationError(f"state database does not exist: {path}")
    connection = _raw_connect(path, existing=True)
    try:
        _validate_schema(connection)
        _configure(connection)
        _private_path(path, parent_created=False)
        return connection
    except BaseException:
        connection.close()
        raise


@contextmanager
def immediate(connection: sqlite3.Connection) -> Iterator[None]:
    """Run a multi-row state change under SQLite's write lock."""

    connection.execute("BEGIN IMMEDIATE")
    try:
        yield
    except BaseException:
        connection.rollback()
        raise
    else:
        connection.commit()
