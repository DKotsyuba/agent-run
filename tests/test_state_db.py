import sqlite3
import stat
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from agent_run.errors import ValidationError
from agent_run.state import SCHEMA_VERSION, initialize_database, open_database


class StateDatabaseTests(unittest.TestCase):
    def test_fresh_init_and_reopen_apply_schema_pragmas_and_private_modes(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            parent = Path(directory) / "private"
            database = parent / "state.db"
            connection = initialize_database(database)
            self.assertEqual(connection.execute("PRAGMA user_version").fetchone()[0], SCHEMA_VERSION)
            self.assertEqual(connection.execute("PRAGMA journal_mode").fetchone()[0], "wal")
            self.assertEqual(connection.execute("PRAGMA synchronous").fetchone()[0], 2)
            self.assertEqual(connection.execute("PRAGMA foreign_keys").fetchone()[0], 1)
            self.assertEqual(connection.execute("PRAGMA busy_timeout").fetchone()[0], 5000)
            connection.close()

            self.assertEqual(stat.S_IMODE(parent.stat().st_mode), 0o700)
            self.assertEqual(stat.S_IMODE(database.stat().st_mode), 0o600)
            reopened = open_database(database)
            self.assertEqual(reopened.execute("PRAGMA foreign_keys").fetchone()[0], 1)
            reopened.close()

    def test_invalid_and_newer_versions_refuse_without_schema_mutation(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            for version in (0, 2):
                with self.subTest(version=version):
                    database = Path(directory) / f"state-{version}.db"
                    connection = sqlite3.connect(database)
                    connection.execute("CREATE TABLE marker (value TEXT)")
                    connection.execute("INSERT INTO marker VALUES ('kept')")
                    connection.execute(f"PRAGMA user_version={version}")
                    connection.commit()
                    connection.close()

                    with self.assertRaises(ValidationError):
                        initialize_database(database)

                    connection = sqlite3.connect(database)
                    self.assertEqual(connection.execute("PRAGMA user_version").fetchone()[0], version)
                    self.assertEqual(connection.execute("SELECT value FROM marker").fetchone()[0], "kept")
                    self.assertEqual(
                        connection.execute(
                            "SELECT COUNT(*) FROM sqlite_master WHERE type='table'"
                        ).fetchone()[0],
                        1,
                    )
                    connection.close()

    def test_open_refuses_missing_or_incomplete_v1_database(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "missing.db"
            with self.assertRaises(ValidationError):
                open_database(database)
            connection = sqlite3.connect(database)
            connection.execute("PRAGMA user_version=1")
            connection.close()
            with self.assertRaises(ValidationError):
                open_database(database)


if __name__ == "__main__":
    unittest.main()
