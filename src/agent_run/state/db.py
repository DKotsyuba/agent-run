"""SQLite connection and schema-version handling."""

from __future__ import annotations

import json
import logging
import math
import os
import sqlite3
import tempfile
import time
import uuid
from contextlib import contextmanager
from functools import lru_cache
from pathlib import Path, PurePosixPath
from typing import Iterator

from agent_run.domain import AgentId, OrchestratorRef, StartRequest
from agent_run.errors import ValidationError
from agent_run.paths import agent_dir as _agent_dir

from .migrations import (
    SCHEMA_VERSION,
    migrate,
    require_current,
    schema_lock as _initialization_lock,
    sql_statements,
    table_names as _tables,
    version_of as _version,
)


MAX_INLINE_MESSAGE_BYTES = 32 * 1024
INLINE_STUB_HEAD_CHARS = 4096
_SCHEMA_PATH = Path(__file__).with_name("schema.sql")
_logger = logging.getLogger("agent_run.state")


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


def _validate_raw_ref(raw_ref: str) -> None:
    if not isinstance(raw_ref, str) or not raw_ref or "\\" in raw_ref:
        raise ValidationError("raw_ref must be a normalized relative path")
    path = PurePosixPath(raw_ref)
    if (
        path.is_absolute()
        or raw_ref != path.as_posix()
        or path.as_posix() == "."
        or ".." in path.parts
        or path.parts[0].startswith("~")
    ):
        raise ValidationError("raw_ref must be a normalized relative path")


def _spool_oversized_message(agent_directory: Path, content: str) -> tuple[str, str]:
    """Spool content over the inline limit to a raw file directly under the
    agent's own directory (same mkstemp-in-place convention as
    adapters/opencode/http.py:_capture, so its bare filename is already a
    normalized raw_ref) and return a bounded inline stub plus that raw_ref.
    """

    agent_directory.mkdir(mode=0o700, parents=True, exist_ok=True)
    encoded = content.encode("utf-8")
    descriptor, name = tempfile.mkstemp(dir=str(agent_directory), prefix="message.", suffix=".raw")
    body_path = Path(name)
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "wb") as sink:
            descriptor = -1
            sink.write(encoded)
            sink.flush()
            os.fsync(sink.fileno())
    except BaseException:
        if descriptor >= 0:
            os.close(descriptor)
        body_path.unlink(missing_ok=True)
        raise
    raw_ref = body_path.name
    stub = (
        f"{content[:INLINE_STUB_HEAD_CHARS]}\n"
        f"[...spooled: {len(encoded)} bytes exceed the 32 KiB inline limit; "
        f"full content in raw_ref={raw_ref}]"
    )
    return stub, raw_ref


def resolve_message_storage(
    content: str, raw_ref: str | None, *, agent_id: AgentId, home: Path
) -> tuple[str, str | None]:
    """Content within the inline limit is returned unchanged; content over it
    is spooled (see :func:`_spool_oversized_message`) so an oversized message
    degrades the row instead of failing the whole write."""

    if raw_ref is not None:
        _validate_raw_ref(raw_ref)
    if len(content.encode("utf-8")) <= MAX_INLINE_MESSAGE_BYTES:
        return content, raw_ref
    return _spool_oversized_message(_agent_dir(agent_id, home), content)


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
            "account": request.account,
        }
    )


def row_dict(row: sqlite3.Row | None) -> dict[str, object] | None:
    return None if row is None else dict(row)


def connection_path(connection: sqlite3.Connection) -> Path:
    """The absolute path SQLite opened this connection's main database from.

    Lets a caller that only holds a connection (not the path used to open it)
    open its own second connection to the same file -- e.g. a StateStore user
    that must hand a same-thread-safe store to a different thread.
    """

    row = connection.execute("PRAGMA database_list").fetchone()
    return Path(str(row["file"]))


def agent_row(
    connection: sqlite3.Connection, agent_id: str | AgentId
) -> sqlite3.Row:
    row = connection.execute("SELECT * FROM agents WHERE id = ?", (agent_id,)).fetchone()
    if row is None:
        raise ValidationError(f"unknown agent: {agent_id}")
    return row


def checked_supervisor_proof(
    agent: sqlite3.Row,
    *,
    verdict: str,
    supervisor_pid: int | None,
    process_group_id: int | None,
    expected_identity: str | None,
    alive: bool | None,
    checked_at: float | None,
    observed_identity: str | None,
) -> tuple[float, str | None]:
    stored = (
        agent["supervisor_pid"],
        agent["process_group_id"],
        agent["supervisor_identity"],
    )
    if any(value is None for value in stored) or agent["heartbeat_at"] is None:
        raise ValidationError("agent has no complete stored supervisor identity")
    if supervisor_pid is None or process_group_id is None or expected_identity is None:
        raise ValidationError("reconciliation proof is incomplete")
    supplied = (
        integer("supervisor_pid", supervisor_pid, minimum=1),
        integer("process_group_id", process_group_id, minimum=1),
        nonblank("expected_identity", expected_identity),
    )
    if supplied != stored:
        raise ValidationError("reconciliation proof does not match stored supervisor")
    if not isinstance(alive, bool):
        raise ValidationError("reconciliation proof must include liveness")
    if checked_at is None:
        raise ValidationError("reconciliation proof must include checked_at")
    checked = timestamp(checked_at)
    if checked < agent["heartbeat_at"]:
        raise ValidationError("reconciliation proof predates the last heartbeat")
    if verdict == "alive":
        if alive is not True:
            raise ValidationError("alive verdict requires a live supervisor proof")
        return checked, None
    if verdict == "dead":
        if alive is not False:
            raise ValidationError("dead verdict requires a not-alive proof")
        return checked, "supervisor_dead"
    if (
        alive is not True
        or not isinstance(observed_identity, str)
        or not observed_identity.strip()
        or observed_identity == expected_identity
    ):
        raise ValidationError("identity mismatch requires differing live identities")
    return checked, "supervisor_identity_mismatch"


def idempotent_agent(
    connection: sqlite3.Connection, request_id: str
) -> sqlite3.Row | None:
    return connection.execute(
        """SELECT id, request_json, task_summary, config_revision FROM agents
           WHERE request_id = ?""",
        (request_id,),
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
        connection.execute(
            """UPDATE orchestrator_sessions
               SET external_turn_id = COALESCE(?, external_turn_id),
                   last_seen_at = MAX(last_seen_at, ?)
               WHERE id = ?""",
            (ref.external_turn_id, at, row["id"]),
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


def _upsert_context_receipt(
    connection: sqlite3.Connection,
    orchestrator_session_id: str,
    context_key: str,
    injected_at: float,
) -> bool:
    return connection.execute(
        """INSERT INTO context_receipts (
               orchestrator_session_id, context_key, injected_at
           ) VALUES (?, ?, ?)
           ON CONFLICT(orchestrator_session_id) DO UPDATE SET
               context_key = excluded.context_key,
               injected_at = excluded.injected_at
           WHERE context_receipts.context_key <> excluded.context_key""",
        (orchestrator_session_id, context_key, injected_at),
    ).rowcount == 1


#: Version tag written into every component-fingerprint context receipt key.
_CONTEXT_RECEIPT_VERSION = 2


def encode_context_components(components: dict[str, str]) -> str:
    """Encode component fingerprints as one versioned, order-independent key.

    ``components`` maps nonblank names to nonblank fingerprint strings. The
    result is canonical JSON (sorted names, fixed separators), so the same
    component set always encodes to the same single ``context_receipts`` key.
    """

    if "v" in components:
        raise ValidationError("component name v is reserved")
    if not all(
        isinstance(name, str) and name.strip() and isinstance(value, str) and value.strip()
        for name, value in components.items()
    ):
        raise ValidationError("context components must use nonblank string names and values")
    return json.dumps(
        {"v": _CONTEXT_RECEIPT_VERSION, "components": components},
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def parse_context_components(context_key: str) -> dict[str, str] | None:
    """Decode a versioned component receipt key back into its fingerprints.

    Returns the name-to-fingerprint mapping for any key produced by
    :func:`encode_context_components`, or ``None`` for anything else --
    legacy single-digest keys, foreign formats, or malformed JSON -- so the
    caller can treat those rows as "every component changed".
    """

    try:
        decoded = json.loads(context_key)
    except (TypeError, ValueError):
        return None
    if not isinstance(decoded, dict) or decoded.get("v") != _CONTEXT_RECEIPT_VERSION:
        return None
    values = decoded.get("components")
    if not isinstance(values, dict) or not values or not all(
        isinstance(name, str) and name.strip() and isinstance(value, str) and value.strip()
        for name, value in values.items()
    ):
        return None
    return values


def record_context_component_receipt(
    connection: sqlite3.Connection,
    orchestrator_session_id: str,
    components: dict[str, str],
    injected_at: float,
) -> frozenset[str]:
    """Compare and store component fingerprints in one already-open write
    transaction, returning the names whose stored fingerprint differs.

    Must run inside the caller's ``BEGIN IMMEDIATE`` block so the read,
    compare, and write are atomic against concurrent callers. A missing row,
    or a row holding a legacy non-versioned key, compares as changed for
    every component and is (re)written in place in the versioned encoding;
    fingerprint values equal to the stored ones neither change the row nor
    touch ``injected_at``. Components absent from ``components`` but present
    in a valid stored row are preserved unchanged.
    """

    row = connection.execute(
        "SELECT context_key FROM context_receipts WHERE orchestrator_session_id = ?",
        (orchestrator_session_id,),
    ).fetchone()
    stored = {} if row is None else (parse_context_components(str(row["context_key"])) or {})
    changed = frozenset(
        name for name, value in components.items() if stored.get(name) != value
    )
    if not changed:
        return changed
    merged = dict(stored)
    merged.update(components)
    connection.execute(
        """INSERT INTO context_receipts (
               orchestrator_session_id, context_key, injected_at
           ) VALUES (?, ?, ?)
           ON CONFLICT(orchestrator_session_id) DO UPDATE SET
               context_key = excluded.context_key,
               injected_at = excluded.injected_at""",
        (orchestrator_session_id, encode_context_components(merged), injected_at),
    )
    return changed


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


def _table_shape(
    connection: sqlite3.Connection, table: str
) -> tuple[tuple[str, str, int, int], ...]:
    return tuple(
        (str(row["name"]), str(row["type"]).upper(), int(row["notnull"]), int(row["pk"]))
        for row in connection.execute(f'PRAGMA table_info("{table}")')
    )


@lru_cache(maxsize=1)
def _expected_shapes() -> dict[str, tuple[tuple[str, str, int, int], ...]]:
    """Table shapes of the current schema, read from schema.sql itself.

    Deriving the table set from the file keeps it from drifting out of step
    with a hand-maintained list every time a migration adds a table.
    """

    reference = sqlite3.connect(":memory:")
    reference.row_factory = sqlite3.Row
    try:
        reference.executescript(_SCHEMA_PATH.read_text(encoding="utf-8"))
        return {table: _table_shape(reference, table) for table in _tables(reference)}
    finally:
        reference.close()


@lru_cache(maxsize=1)
def _schema_statements() -> tuple[str, ...]:
    return sql_statements(_SCHEMA_PATH.read_text(encoding="utf-8"), "schema.sql")


def _validate_schema(connection: sqlite3.Connection) -> None:
    expected = _expected_shapes()
    version = _version(connection)
    require_current(version)
    if version != SCHEMA_VERSION or not expected.keys() <= _tables(connection):
        raise ValidationError(
            f"unsupported or incomplete state schema: version {version}; "
            f"expected {SCHEMA_VERSION}"
        )
    if any(_table_shape(connection, table) != shape for table, shape in expected.items()):
        raise ValidationError(
            f"state schema columns or primary keys do not match schema v{SCHEMA_VERSION}"
        )


def _initialize_schema(connection: sqlite3.Connection) -> None:
    connection.execute("BEGIN IMMEDIATE")
    try:
        version = _version(connection)
        tables = _tables(connection)
        if version == 0 and not tables:
            for statement in _schema_statements():
                connection.execute(statement)
        else:
            _validate_schema(connection)
        _validate_schema(connection)
    except BaseException:
        connection.rollback()
        raise
    else:
        connection.commit()


def _configure(connection: sqlite3.Connection) -> None:
    mode = connection.execute("PRAGMA journal_mode=WAL").fetchone()[0]
    if str(mode).lower() != "wal":
        raise ValidationError(f"state database could not enable WAL mode: {mode}")
    connection.execute("PRAGMA synchronous=FULL")
    connection.execute("PRAGMA foreign_keys=ON")
    connection.execute("PRAGMA busy_timeout=5000")


def _private_path(path: Path, *, parent_created: bool, strict: bool) -> None:
    """Tighten the store file, its WAL/SHM siblings and (if just made) its
    parent directory to owner-only modes.

    ``strict`` is True when this process created the database: a chmod failure
    then propagates, because a store left world-readable at creation is a
    defect. It is False for a pre-existing database: the chmod is best effort,
    so a sandboxed reader (read-only filesystem, ``PermissionError`` on chmod)
    can still open a store someone else created. A missing sibling is never an
    error in either mode. Failures skipped in best-effort mode are logged at
    debug on ``agent_run.state``.
    """

    if parent_created:
        path.parent.chmod(0o700)
    for candidate in (path, Path(f"{path}-wal"), Path(f"{path}-shm")):
        try:
            candidate.chmod(0o600)
        except FileNotFoundError:
            pass
        except OSError as error:
            if strict:
                raise
            _logger.debug("db=%s chmod skipped: %s", candidate, error)


def initialize_database(database: str | Path) -> sqlite3.Connection:
    """Create the current schema, or migrate and open an older database."""

    path = Path(database).expanduser().resolve()
    parent_created = not path.parent.exists()
    path.parent.mkdir(parents=True, exist_ok=True)
    if parent_created:
        path.parent.chmod(0o700)
    existed = path.exists()
    if existed and path.stat().st_size:
        migrate(path)
    connection = _raw_connect(path)
    try:
        _private_path(path, parent_created=parent_created, strict=not existed)
        version = _version(connection)
        tables = _tables(connection)
        if version != 0 or tables:
            _validate_schema(connection)
        with _initialization_lock(path):
            _initialize_schema(connection)
            _configure(connection)
        _private_path(path, parent_created=parent_created, strict=not existed)
        return connection
    except BaseException:
        connection.close()
        if not existed and path.exists() and path.stat().st_size == 0:
            path.unlink()
        raise


def open_database(database: str | Path) -> sqlite3.Connection:
    """Open an existing database, migrating it up to the current schema first.

    An older home therefore needs no manual step; a newer one is refused by
    :func:`migrate` rather than silently opened.

    Re-tightening the file modes is best effort here: the database already
    exists, so a caller that may not chmod it (sandboxed read-only child) still
    gets a connection instead of a ``PermissionError``.
    """

    path = Path(database).expanduser().resolve()
    if not path.is_file():
        raise ValidationError(f"state database does not exist: {path}")
    _private_path(path, parent_created=False, strict=False)
    migrate(path)
    connection = _raw_connect(path, existing=True)
    try:
        _validate_schema(connection)
        _configure(connection)
        _private_path(path, parent_created=False, strict=False)
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
