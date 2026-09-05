"""Numbered, transactional, backup-backed state schema migrations.

Every migration is a numbered SQL file in ``migrations/`` named
``NNN_slug.sql``, where ``NNN`` is the schema version the file produces.
Version 1 has no file: it is created wholesale from ``schema.sql``, which
always describes the *current* schema.  A migration file therefore holds only
the delta, and ``tests/test_state_migrations.py`` proves a migrated store and
a freshly created one end up byte-identical.

Each migration runs inside one ``BEGIN IMMEDIATE`` transaction, so an
interrupted migration leaves the store at its previous version rather than
half-upgraded.  Before the transaction opens, the store is snapshotted next to
itself through SQLite's own backup API.  The backup API copies the database
under a read transaction and is therefore consistent in WAL mode; a plain file
copy is not, because committed pages may still live only in the ``-wal``
sidecar.  The snapshot is removed only once the transaction commits.
"""

from __future__ import annotations

import fcntl
import logging
import os
import re
import sqlite3
from contextlib import contextmanager
from functools import lru_cache
from pathlib import Path
from typing import Iterator

from agent_run.errors import SchemaMigrationRequired, ValidationError

_logger = logging.getLogger("agent_run.state")

SCHEMA_VERSION = 12

# The tables schema v1 created.  A store stamped with a version at or above 1
# must still have all of them before any migration is allowed to touch it, so
# that a truncated or foreign database is refused instead of upgraded.
V1_TABLES = frozenset(
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

_MIGRATIONS_DIR = Path(__file__).with_name("migrations")
_FILE_NAME = re.compile(r"^(\d{3})_[a-z0-9_]+\.sql$")


def version_of(connection: sqlite3.Connection) -> int:
    return int(connection.execute("PRAGMA user_version").fetchone()[0])


def table_names(connection: sqlite3.Connection) -> frozenset[str]:
    rows = connection.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'"
    )
    return frozenset(row[0] for row in rows)


def sql_statements(text: str, source: str) -> tuple[str, ...]:
    """Split a SQL script into single statements.

    ``executescript`` would COMMIT before it runs, which would drop a
    migration out of its own transaction, so every statement is executed
    individually instead.
    """

    statements: list[str] = []
    pending = ""
    for line in text.splitlines(keepends=True):
        pending += line
        if sqlite3.complete_statement(pending):
            statements.append(pending)
            pending = ""
    if pending.strip():
        raise ValidationError(f"{source} contains an incomplete statement")
    return tuple(statements)


@lru_cache(maxsize=1)
def pending_files() -> tuple[tuple[int, Path], ...]:
    """Every migration file, ordered by the version it produces."""

    found: list[tuple[int, Path]] = []
    for path in sorted(_MIGRATIONS_DIR.glob("*.sql")):
        match = _FILE_NAME.match(path.name)
        if match is None:
            raise ValidationError(f"migration file is not named NNN_slug.sql: {path.name}")
        found.append((int(match.group(1)), path))
    versions = [version for version, _ in found]
    if versions != list(range(2, SCHEMA_VERSION + 1)):
        raise ValidationError(
            f"migrations must cover versions 2..{SCHEMA_VERSION}, found {versions}"
        )
    return tuple(found)


def backup_path(store_path: Path, target: int) -> Path:
    return store_path.with_name(f"{store_path.name}.pre-v{target}.backup")


@contextmanager
def schema_lock(path: Path) -> Iterator[None]:
    """Serialize schema creation and migration across processes."""

    lock_path = path.with_name(f".{path.name}.init.lock")
    descriptor = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o600)
    try:
        os.fchmod(descriptor, 0o600)
        fcntl.flock(descriptor, fcntl.LOCK_EX)
        yield
    finally:
        fcntl.flock(descriptor, fcntl.LOCK_UN)
        os.close(descriptor)


def _connect(path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(path, timeout=30.0, isolation_level=None)
    connection.row_factory = sqlite3.Row
    # Match the durability the store is normally opened with; a migration
    # commit is the one write nobody can replay.
    connection.execute("PRAGMA synchronous=FULL")
    connection.execute("PRAGMA busy_timeout=30000")
    return connection


def _refuse_newer(path: Path, version: int) -> None:
    if version > SCHEMA_VERSION:
        raise ValidationError(
            f"state database {path} is schema v{version}, newer than this agent-run "
            f"understands (v{SCHEMA_VERSION}); upgrade agent-run instead of downgrading "
            "the store"
        )


def _drop_stale_backups(path: Path) -> None:
    """Remove snapshots left by a migration that committed but died before cleanup.

    Only versions this binary understands are dropped: during a rollout an older
    binary runs beside a newer one, and a ``pre-v*`` glob without the version
    guard deletes the newer binary's in-flight snapshot out from under it.
    """

    for stale in path.parent.glob(f"{path.name}.pre-v*.backup"):
        suffix = stale.name[len(path.name) + len(".pre-v") : -len(".backup")]
        if suffix.isdigit() and int(suffix) <= SCHEMA_VERSION:
            stale.unlink(missing_ok=True)


def _snapshot(connection: sqlite3.Connection, path: Path, target: int) -> Path:
    backup = backup_path(path, target)
    backup.unlink(missing_ok=True)
    destination = sqlite3.connect(backup)
    try:
        connection.backup(destination)
    finally:
        destination.close()
    backup.chmod(0o600)
    return backup


def _apply_one(
    connection: sqlite3.Connection, path: Path, target: int, source: Path
) -> None:
    """Apply one numbered migration atomically under safe SQLite rebuild settings.

    The connection temporarily disables foreign-key enforcement and enables
    legacy ALTER TABLE behavior so table renames cannot rewrite references in
    other tables.  The migration is rolled back if execution or the explicit
    foreign-key audit fails, and both connection settings are restored before
    returning or raising :class:`ValidationError`.
    """

    backup = _snapshot(connection, path, target)
    statements = sql_statements(source.read_text(encoding="utf-8"), source.name)
    foreign_keys = int(connection.execute("PRAGMA foreign_keys").fetchone()[0])
    legacy_alter_table = int(
        connection.execute("PRAGMA legacy_alter_table").fetchone()[0]
    )
    try:
        connection.execute("PRAGMA foreign_keys=OFF")
        connection.execute("PRAGMA legacy_alter_table=ON")
        try:
            connection.execute("BEGIN IMMEDIATE")
            try:
                for statement in statements:
                    connection.execute(statement)
                violations = connection.execute("PRAGMA foreign_key_check").fetchall()
                if violations:
                    raise sqlite3.IntegrityError(
                        f"foreign_key_check found {len(violations)} violation(s)"
                    )
                connection.execute(f"PRAGMA user_version={int(target)}")
            except BaseException:
                connection.rollback()
                raise
            connection.commit()
        finally:
            connection.execute(f"PRAGMA legacy_alter_table={legacy_alter_table}")
            connection.execute(f"PRAGMA foreign_keys={foreign_keys}")
    except BaseException as error:
        _logger.warning("db=%s migration_failed target=%d error_kind=%s", path, target, type(error).__name__)
        raise ValidationError(
            f"state schema migration to v{target} failed and was rolled back: {error}. "
            f"The pre-migration backup of {path} is intact at {backup}"
        ) from error
    _logger.info("db=%s migration_applied target=%d source=%s", path, target, source.name)
    backup.unlink(missing_ok=True)


def _apply_pending(path: Path) -> int:
    connection = _connect(path)
    try:
        version = version_of(connection)
        _refuse_newer(path, version)
        for target, source in pending_files():
            if target <= version:
                continue
            _apply_one(connection, path, target, source)
            version = target
        return version
    finally:
        connection.close()


def migrate(store_path: str | Path) -> int:
    """Bring an existing store up to :data:`SCHEMA_VERSION` and return its version.

    Returns 0 for a database that carries no version stamp at all -- it is not
    an agent-run store, and the caller's schema validation owns that refusal.
    """

    path = Path(store_path).expanduser().resolve()
    if not path.is_file():
        raise ValidationError(f"state database does not exist: {path}")
    connection = _connect(path)
    try:
        version = version_of(connection)
        _logger.debug("db=%s user_version=%d target=%d", path, version, SCHEMA_VERSION)
        if version == 0:
            return 0
        _refuse_newer(path, version)
        if version == SCHEMA_VERSION:
            _drop_stale_backups(path)
            return version
        missing = V1_TABLES - table_names(connection)
        if missing:
            raise ValidationError(
                f"state database {path} claims schema v{version} but is missing "
                f"{', '.join(sorted(missing))}; refusing to migrate an incomplete store"
            )
    finally:
        connection.close()
    with schema_lock(path):
        applied = _apply_pending(path)
        _logger.info(
            "db=%s migrated user_version_before=%d user_version_after=%d",
            path, version, applied,
        )
        return applied


def require_current(version: int) -> None:
    """Raise the read-only counterpart of :func:`migrate` for an old store."""

    if 0 < version < SCHEMA_VERSION:
        raise SchemaMigrationRequired(version, SCHEMA_VERSION)
