import re
import sqlite3
import sys
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Barrier

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from agent_run.doctor import run_doctor
from agent_run.errors import SchemaMigrationRequired, ValidationError
from agent_run.state import (
    SCHEMA_VERSION,
    backup_path,
    diagnostic_snapshot,
    initialize_database,
    migrate,
    open_database,
)
from agent_run.state.migrations import pending_files
from agent_run.state.migrations import _apply_one
from agent_run.state.store import StateStore

# The byte-for-byte schema.sql that shipped as v1, kept so these tests migrate a
# real v1 store rather than a hand-rolled approximation of one.
_V1_SCHEMA = Path(__file__).resolve().parent / "fixtures" / "schema_v1.sql"
# Same idea, one version later: schema.sql as it shipped as v2, before 003
# added workflow_runs.plan_json.
_V2_SCHEMA = Path(__file__).resolve().parent / "fixtures" / "schema_v2.sql"
_AGENTS = ("agt_alpha", "agt_beta", "agt_gamma")


def _build_v1_store(path: Path, *, extra: str | None = None) -> None:
    connection = sqlite3.connect(path)
    try:
        connection.executescript(_V1_SCHEMA.read_text(encoding="utf-8"))
        for index, agent_id in enumerate(_AGENTS):
            connection.execute(
                """INSERT INTO agents
                   (id, runtime, model, profile, task, task_summary, workdir,
                    request_json, status, created_at, timeout_seconds, config_revision)
                   VALUES (?, 'codex', 'model', 'profile', 'task', 'summary', '/tmp',
                           '{}', 'running', ?, 10.0, 'cfg')""",
                (agent_id, float(index)),
            )
            connection.execute(
                "INSERT INTO events (agent_id, at, kind) VALUES (?, 1.0, 'created')",
                (agent_id,),
            )
        if extra is not None:
            connection.execute(extra)
        connection.commit()
    finally:
        connection.close()


def _build_v2_store(path: Path) -> str:
    """A real v2 store with one resumable run, returning that run's id."""

    connection = sqlite3.connect(path)
    try:
        connection.executescript(_V2_SCHEMA.read_text(encoding="utf-8"))
        connection.execute(
            """INSERT INTO workflow_runs
               (id, name, script_sha, status, created_at)
               VALUES ('wr_1', 'flow', 'sha', 'failed', 1.0)"""
        )
        connection.commit()
    finally:
        connection.close()
    return "wr_1"


def _scalar(path: Path, sql: str) -> object:
    connection = sqlite3.connect(f"{path.as_uri()}?mode=ro", uri=True)
    try:
        return connection.execute(sql).fetchone()[0]
    finally:
        connection.close()


def _schema_objects(connection: sqlite3.Connection) -> list[tuple[str, str, str]]:
    """Return every application table/index with deliberately normalized SQL.

    Whitespace and SQLite-added identifier quotes are ignored, while all other
    CREATE text remains significant so foreign-key targets cannot drift.
    """

    def normalized(sql: object) -> str:
        """Normalize insignificant CREATE-SQL whitespace and identifier quotes."""

        text = re.sub(r'"([A-Za-z_][A-Za-z0-9_]*)"', r"\1", str(sql or ""))
        return " ".join(text.split())

    return sorted(
        (str(row[0]), str(row[1]), normalized(row[2]))
        for row in connection.execute(
            """SELECT type, name, sql FROM sqlite_master
               WHERE type IN ('table', 'index') AND name NOT LIKE 'sqlite_%'"""
        )
    )


def _build_poisoned_v5_store(path: Path, run_id: str) -> None:
    """Create a v5 store whose workflow foreign keys name a dropped rebuild table."""

    connection = initialize_database(path)
    try:
        connection.execute(
            """INSERT INTO workflow_runs
               (id, name, script_sha, status, owner_pid_identity, created_at)
               VALUES (?, 'flow', 'sha', 'running', '999999 dead-owner', 1.0)""",
            (run_id,),
        )
        connection.commit()
    finally:
        connection.close()

    migration_five = dict(pending_files())[5]
    connection = sqlite3.connect(path)
    try:
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA legacy_alter_table=OFF")
        connection.executescript(migration_five.read_text(encoding="utf-8"))
        # Downgrade the stamp and the post-v5 tables afterwards: replaying 005
        # is what proves 006's repair, and the store must look exactly like a
        # genuine v5 one so migrations 006..end can re-run over it.
        connection.execute("DROP TABLE run_stats")
        connection.execute("PRAGMA user_version=5")
        connection.commit()
    finally:
        connection.close()


class MigrationRegistryTests(unittest.TestCase):
    def test_files_are_numbered_and_cover_every_version_after_one(self) -> None:
        self.assertEqual(
            [version for version, _ in pending_files()],
            list(range(2, SCHEMA_VERSION + 1)),
        )

    def test_stale_backup_cleanup_spares_newer_versions_snapshots(self) -> None:
        from agent_run.state.migrations import _drop_stale_backups

        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "state.db"
            own = database.with_name(f"state.db.pre-v{SCHEMA_VERSION}.backup")
            newer = database.with_name(f"state.db.pre-v{SCHEMA_VERSION + 1}.backup")
            own.touch()
            newer.touch()

            _drop_stale_backups(database)

            self.assertFalse(own.exists())
            self.assertTrue(newer.exists())

    def test_foreign_key_check_is_clean_after_each_migration(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "state.db"
            _build_v1_store(database)
            connection = sqlite3.connect(database, isolation_level=None)
            try:
                connection.execute("PRAGMA foreign_keys=ON")
                connection.execute("PRAGMA legacy_alter_table=OFF")
                for target, source in pending_files():
                    _apply_one(connection, database, target, source)
                    self.assertEqual(
                        connection.execute("PRAGMA foreign_key_check").fetchall(), []
                    )
                    self.assertEqual(
                        connection.execute("PRAGMA foreign_keys").fetchone()[0], 1
                    )
                    self.assertEqual(
                        connection.execute("PRAGMA legacy_alter_table").fetchone()[0],
                        0,
                    )
            finally:
                connection.close()

    def test_open_repairs_poisoned_v5_store_before_reconciliation(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "state.db"
            run_id = "wf_poisoned"
            _build_poisoned_v5_store(database, run_id)

            store = StateStore.open(database)
            try:
                self.assertEqual(
                    store.connection.execute(
                        "SELECT status FROM workflow_runs WHERE id = ?", (run_id,)
                    ).fetchone()[0],
                    "lost",
                )
                store.connection.execute(
                    """INSERT INTO workflow_steps
                       (run_id, step_key, spec_json, status)
                       VALUES (?, 'step', '{}', 'pending')""",
                    (run_id,),
                )
                store.connection.commit()
                self.assertEqual(
                    store.connection.execute("PRAGMA foreign_key_check").fetchall(), []
                )
            finally:
                store.close()


class V1UpgradeTests(unittest.TestCase):
    def test_v1_home_opens_transparently_and_keeps_its_rows(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "state.db"
            _build_v1_store(database)
            self.assertEqual(_scalar(database, "PRAGMA user_version"), 1)
            before = _scalar(database, "SELECT COUNT(*) FROM agents")

            connection = open_database(database)
            try:
                self.assertEqual(
                    connection.execute("PRAGMA user_version").fetchone()[0],
                    SCHEMA_VERSION,
                )
                self.assertEqual(
                    connection.execute("SELECT COUNT(*) FROM agents").fetchone()[0],
                    before,
                )
                self.assertEqual(
                    connection.execute("SELECT COUNT(*) FROM events").fetchone()[0],
                    len(_AGENTS),
                )
                connection.execute(
                    """INSERT INTO workflow_runs
                       (id, name, script_sha, status, created_at)
                       VALUES ('wr_1', 'flow', 'sha', 'created', 1.0)"""
                )
                connection.execute(
                    """INSERT INTO workflow_steps
                       (run_id, step_key, spec_json, agent_id, status)
                       VALUES ('wr_1', 'step', '{}', ?, 'pending')""",
                    (_AGENTS[0],),
                )
                connection.commit()
            finally:
                connection.close()

            self.assertFalse(backup_path(database, SCHEMA_VERSION).exists())

    def test_migrated_store_is_indistinguishable_from_a_fresh_one(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            migrated = Path(directory) / "migrated.db"
            _build_v1_store(migrated)
            self.assertEqual(migrate(migrated), SCHEMA_VERSION)

            fresh = Path(directory) / "fresh.db"
            fresh_connection = initialize_database(fresh)
            migrated_connection = sqlite3.connect(migrated)
            try:
                self.assertEqual(
                    _schema_objects(migrated_connection),
                    _schema_objects(fresh_connection),
                )
            finally:
                fresh_connection.close()
                migrated_connection.close()

    def test_workflow_status_enums_are_enforced(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "state.db"
            _build_v1_store(database)
            connection = open_database(database)
            try:
                with self.assertRaises(sqlite3.IntegrityError):
                    connection.execute(
                        """INSERT INTO workflow_runs
                           (id, name, script_sha, status, created_at)
                           VALUES ('wr_1', 'flow', 'sha', 'bogus', 1.0)"""
                    )
                connection.execute(
                    """INSERT INTO workflow_runs
                       (id, name, script_sha, status, created_at)
                       VALUES ('wr_1', 'flow', 'sha', 'running', 1.0)"""
                )
                with self.assertRaises(sqlite3.IntegrityError):
                    connection.execute(
                        """INSERT INTO workflow_steps
                           (run_id, step_key, spec_json, status)
                           VALUES ('wr_1', 'step', '{}', 'bogus')"""
                    )
            finally:
                connection.close()

    def test_initialize_also_upgrades_an_existing_v1_home(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "state.db"
            _build_v1_store(database)
            connection = initialize_database(database)
            try:
                self.assertEqual(
                    connection.execute("PRAGMA user_version").fetchone()[0],
                    SCHEMA_VERSION,
                )
                self.assertEqual(
                    connection.execute("SELECT COUNT(*) FROM agents").fetchone()[0],
                    len(_AGENTS),
                )
            finally:
                connection.close()

    def test_concurrent_openers_migrate_a_v1_store_exactly_once(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "state.db"
            _build_v1_store(database)
            callers = 8
            barrier = Barrier(callers)

            def open_once(_: int) -> int:
                barrier.wait()
                connection = open_database(database)
                try:
                    return connection.execute("PRAGMA user_version").fetchone()[0]
                finally:
                    connection.close()

            with ThreadPoolExecutor(max_workers=callers) as pool:
                versions = list(pool.map(open_once, range(callers)))

            self.assertEqual(versions, [SCHEMA_VERSION] * callers)
            self.assertEqual(
                _scalar(database, "SELECT COUNT(*) FROM agents"), len(_AGENTS)
            )
            self.assertFalse(backup_path(database, SCHEMA_VERSION).exists())

    def test_migration_is_idempotent_and_clears_a_stale_backup(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "state.db"
            _build_v1_store(database)
            self.assertEqual(migrate(database), SCHEMA_VERSION)
            # A crash after the transaction commits but before cleanup.
            stale = backup_path(database, SCHEMA_VERSION)
            stale.write_bytes(b"stale")
            self.assertEqual(migrate(database), SCHEMA_VERSION)
            self.assertFalse(stale.exists())


class V2UpgradeTests(unittest.TestCase):
    def test_v2_home_opens_to_v3_with_plan_json_present(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "state.db"
            run_id = _build_v2_store(database)
            self.assertEqual(_scalar(database, "PRAGMA user_version"), 2)

            connection = open_database(database)
            try:
                self.assertEqual(
                    connection.execute("PRAGMA user_version").fetchone()[0],
                    SCHEMA_VERSION,
                )
                columns = {
                    row[1]
                    for row in connection.execute("PRAGMA table_info(workflow_runs)")
                }
                self.assertIn("plan_json", columns)
                # A row that predates the column reads back NULL, not an error.
                self.assertIsNone(
                    connection.execute(
                        "SELECT plan_json FROM workflow_runs WHERE id = ?", (run_id,)
                    ).fetchone()[0]
                )
                connection.execute(
                    "UPDATE workflow_runs SET plan_json = '[]' WHERE id = ?", (run_id,)
                )
                connection.commit()
            finally:
                connection.close()

    def test_v2_migrated_store_is_indistinguishable_from_a_fresh_one(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            migrated = Path(directory) / "migrated.db"
            _build_v2_store(migrated)
            self.assertEqual(migrate(migrated), SCHEMA_VERSION)

            fresh = Path(directory) / "fresh.db"
            fresh_connection = initialize_database(fresh)
            migrated_connection = sqlite3.connect(migrated)
            try:
                self.assertEqual(
                    _schema_objects(migrated_connection),
                    _schema_objects(fresh_connection),
                )
            finally:
                fresh_connection.close()
                migrated_connection.close()


class MigrationRefusalTests(unittest.TestCase):
    def test_newer_schema_is_refused_without_touching_the_store(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "state.db"
            _build_v1_store(database)
            connection = sqlite3.connect(database)
            connection.execute(f"PRAGMA user_version={SCHEMA_VERSION + 1}")
            connection.close()

            with self.assertRaisesRegex(ValidationError, "newer than this agent-run"):
                open_database(database)
            with self.assertRaisesRegex(ValidationError, "newer than this agent-run"):
                initialize_database(database)

            self.assertEqual(_scalar(database, "PRAGMA user_version"), SCHEMA_VERSION + 1)
            self.assertEqual(
                _scalar(database, "SELECT COUNT(*) FROM agents"), len(_AGENTS)
            )
            self.assertFalse(backup_path(database, SCHEMA_VERSION).exists())

    def test_incomplete_store_claiming_a_version_is_refused_unmigrated(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "state.db"
            connection = sqlite3.connect(database)
            connection.execute("CREATE TABLE agents (id TEXT PRIMARY KEY)")
            connection.execute("PRAGMA user_version=1")
            connection.close()

            with self.assertRaisesRegex(ValidationError, "refusing to migrate"):
                open_database(database)
            self.assertEqual(_scalar(database, "PRAGMA user_version"), 1)

    def test_failed_migration_rolls_back_and_leaves_the_backup(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "state.db"
            # A table the migration is about to create: CREATE TABLE fails, so
            # the whole migration transaction rolls back.
            _build_v1_store(database, extra="CREATE TABLE workflow_runs (wrong TEXT)")

            with self.assertRaises(ValidationError) as caught:
                open_database(database)
            message = str(caught.exception)
            # The failing step is v1->v2, so the surviving backup is pre-v2 --
            # not pre-v<terminal>: later migrations never ran.
            backup = backup_path(database, 2)
            self.assertIn(str(backup), message)
            self.assertIn("rolled back", message)

            self.assertTrue(backup.is_file())
            self.assertEqual(_scalar(backup, "PRAGMA user_version"), 1)
            self.assertEqual(
                _scalar(backup, "SELECT COUNT(*) FROM agents"), len(_AGENTS)
            )
            # The store itself is untouched, so a retry starts from the same place.
            self.assertEqual(_scalar(database, "PRAGMA user_version"), 1)
            self.assertEqual(
                _scalar(database, "SELECT COUNT(*) FROM agents"), len(_AGENTS)
            )


class MigrationDiagnosticsTests(unittest.TestCase):
    def test_read_only_snapshot_reports_the_pending_migration(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "state.db"
            _build_v1_store(database)
            with self.assertRaises(SchemaMigrationRequired) as caught:
                diagnostic_snapshot(database, at=1.0)
            self.assertEqual(caught.exception.found, 1)
            self.assertEqual(caught.exception.expected, SCHEMA_VERSION)
            # A read-only caller must never migrate on the reader's behalf.
            self.assertEqual(_scalar(database, "PRAGMA user_version"), 1)

    def test_doctor_surfaces_a_pending_migration(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            home = Path(directory).resolve()
            (home / "config.toml").write_text("schema_version = 1\n", encoding="utf-8")
            _build_v1_store(home / "state.db")

            report = run_doctor(home, at=1.0)
            findings = [item for item in report.findings if item.component == "state"]
            self.assertEqual([item.code for item in findings], ["state_migration_pending"])
            self.assertIn(f"v{SCHEMA_VERSION}", findings[0].detail)
            self.assertFalse(report.ok)


if __name__ == "__main__":
    unittest.main()
