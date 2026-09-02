"""Reproduction and regression coverage for the OmniRoute current-quota fix.

Before this fix, ``omniroute.pool_samples`` read the ``quota_snapshots``
history table, which OmniRoute's own sync can leave unchanged for well over
``omniroute.LIMITS_STALE_SECONDS`` even while a fresher reading sits in the
``key_value`` / ``providerLimitsCache`` current-quota cache. These tests
prove three things: the SQL member query the runtime now uses reads that
current cache via a bounded LEFT JOIN (never quota_snapshots, never an inner
join that would drop a cache-less active member); ``pool_samples`` treats a
member whose cache the query could not resolve as an explicit source failure
rather than a silently shrunk pool; and the embedded Node sanitizer in
``omniroute._DOCKER_SCRIPT`` -- executed here for real, in a local Node
process, against a fake database handle -- projects each cache row down to
type-checked fields with no raw value ever crossing into its stdout output.
"""

from __future__ import annotations

import json
import shutil
import sqlite3
import subprocess
import unittest
from datetime import datetime, timedelta, timezone

from agent_run.adapters import omniroute
from agent_run.adapters.omniroute import CapacitySourceError, pool_samples


def _connect(schema_sql: str) -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.executescript(schema_sql)
    return conn


_SCHEMA = """
CREATE TABLE provider_connections (
    id TEXT PRIMARY KEY,
    provider TEXT NOT NULL,
    is_active INTEGER NOT NULL,
    quota_visible INTEGER NOT NULL
);
CREATE TABLE key_value (
    namespace TEXT NOT NULL,
    key TEXT NOT NULL,
    value TEXT NOT NULL,
    PRIMARY KEY (namespace, key)
);
CREATE TABLE quota_snapshots (
    id INTEGER PRIMARY KEY,
    provider TEXT NOT NULL,
    connection_id TEXT NOT NULL,
    window_key TEXT NOT NULL,
    remaining_percentage REAL NOT NULL,
    next_reset_at TEXT,
    created_at TEXT NOT NULL
);
"""


class MembersQueryTests(unittest.TestCase):
    """Prove the SQL the runtime sends into the container is correct."""

    def setUp(self):
        self.conn = _connect(_SCHEMA)
        self.addCleanup(self.conn.close)

    def _rows(self):
        # ``_MEMBERS_QUERY`` selects only ``cache_json`` -- the connection id
        # is used to join and is deliberately not part of the result -- so
        # these tests read back a plain list of cache values, distinguishing
        # members (where needed) by the cache content itself.
        cur = self.conn.execute(
            omniroute._MEMBERS_QUERY,
            (omniroute._CACHE_NAMESPACE, omniroute.PROVIDER),
        )
        return [row[0] for row in cur.fetchall()]

    def test_reads_the_current_cache_and_ignores_stale_unchanged_history(self):
        # A history row over 90 minutes old with an unchanged value: the old
        # bug's exact reproduction shape.
        stale_created = (
            datetime.now(timezone.utc) - timedelta(minutes=95)
        ).isoformat()
        self.conn.execute(
            "INSERT INTO provider_connections VALUES ('conn-1', 'opencode-go', 1, 1)"
        )
        self.conn.execute(
            "INSERT INTO quota_snapshots "
            "(provider, connection_id, window_key, remaining_percentage, next_reset_at, created_at) "
            "VALUES ('opencode-go', 'conn-1', 'session', 10.0, NULL, ?)",
            (stale_created,),
        )
        fresh_cache = (
            '{"quotas": {"session": {"remainingPercentage": 91.0, '
            '"resetAt": "2026-08-24T19:20:58.435Z"}}, '
            '"fetchedAt": "2026-08-24T19:17:02.435Z", "source": "scheduled"}'
        )
        self.conn.execute(
            "INSERT INTO key_value VALUES ('providerLimitsCache', 'conn-1', ?)",
            (fresh_cache,),
        )
        rows = self._rows()
        self.assertEqual(rows, [fresh_cache])

    def test_active_quota_visible_members_included_others_excluded(self):
        self.conn.executemany(
            "INSERT INTO provider_connections VALUES (?, ?, ?, ?)",
            [
                ("visible", "opencode-go", 1, 1),
                ("inactive", "opencode-go", 0, 1),
                ("hidden", "opencode-go", 1, 0),
                ("other-provider", "codex", 1, 1),
            ],
        )
        for connection_id in ("visible", "inactive", "hidden", "other-provider"):
            self.conn.execute(
                "INSERT INTO key_value VALUES ('providerLimitsCache', ?, ?)",
                (connection_id, json.dumps({"marker": connection_id})),
            )
        rows = self._rows()
        self.assertEqual(rows, [json.dumps({"marker": "visible"})])

    def test_left_join_keeps_an_active_member_with_no_cache_row(self):
        self.conn.execute(
            "INSERT INTO provider_connections VALUES ('no-cache', 'opencode-go', 1, 1)"
        )
        rows = self._rows()
        self.assertEqual(rows, [None])


#: What the embedded Node sanitizer emits for a member whose cache row is
#: missing, unparseable, or has no usable ``quotas`` object: one row per
#: known window with a null percentage, so the failure is visible data
#: instead of a member that quietly stopped contributing to the average.
def _poison_rows(fetched_at=None):
    return [
        {"window_key": key, "remaining_percentage": None, "next_reset_at": None,
         "fetched_at": fetched_at}
        for key in omniroute.WINDOWS
    ]


class SanitizedRowContractTests(unittest.TestCase):
    """Prove pool_samples treats sanitizer failure markers as source errors."""

    def setUp(self):
        self.now = datetime.fromisoformat("2026-08-28T14:06:01.922Z").timestamp() + 60.0

    def test_a_missing_or_malformed_member_cache_fails_the_whole_pool(self):
        good = {"window_key": "session", "remaining_percentage": 90.0,
                "next_reset_at": None, "fetched_at": "2026-08-28T14:06:01.922Z"}
        with self.assertRaises(CapacitySourceError) as raised:
            pool_samples(self.now, fetch_rows=lambda: [good, *_poison_rows()])
        self.assertEqual(raised.exception.reason, "omniroute_malformed_data")

    def test_a_reset_only_change_is_visible_from_the_current_cache_alone(self):
        # No new "history" row at all -- only a cache read with a moved
        # reset -- must still change what pool_samples reports.
        earlier_reset = [{"window_key": "session", "remaining_percentage": 80.0,
                           "next_reset_at": "2026-08-24T19:20:58.435Z",
                           "fetched_at": "2026-08-24T19:17:02.435Z"}]
        later_reset = [{"window_key": "session", "remaining_percentage": 80.0,
                         "next_reset_at": "2026-08-24T20:20:58.435Z",
                         "fetched_at": "2026-08-24T19:17:02.435Z"}]
        now = datetime.fromisoformat("2026-08-24T19:17:02.435Z").timestamp() + 60.0
        (before,) = pool_samples(now, fetch_rows=lambda: earlier_reset)
        (after,) = pool_samples(now, fetch_rows=lambda: later_reset)
        self.assertNotEqual(before.reset_at, after.reset_at)
        self.assertEqual(after.reset_at, datetime.fromisoformat("2026-08-24T20:20:58.435Z"))


#: A minimal in-process stand-in for OmniRoute's ``better-sqlite3`` handle:
#: only the ``prepare(sql).all(...)`` shape the embedded script calls is
#: implemented, and it ignores ``sql``/bind params entirely -- the fixed
#: ``rows`` it was built with are returned unconditionally, since these
#: tests only need to control what ``members`` the sanitizer sees.
_FAKE_DB_HARNESS = """
function require(name) {
  return function Database(path, opts) {
    return {
      prepare: function(sql) {
        return { all: function() { return __FAKE_MEMBERS__; } };
      }
    };
  };
}
"""


def _run_docker_script(members):
    """Execute the real ``_DOCKER_SCRIPT`` in a local Node process.

    ``members`` is the list of ``{cache_json: ...}`` rows a fake
    ``better-sqlite3`` handle would return; it is spliced into a small
    ``require()`` stub so the actual sanitizer body under test -- taken
    verbatim from ``omniroute._DOCKER_SCRIPT`` -- runs unmodified against it,
    with no real Docker container and no SQLite file on disk. Returns the
    parsed JSON array the script prints to stdout. Requires a ``node``
    binary on ``PATH``; callers should skip when it is absent.
    """

    harness = _FAKE_DB_HARNESS.replace(
        "__FAKE_MEMBERS__", json.dumps(members)
    )
    script = harness + omniroute._DOCKER_SCRIPT
    result = subprocess.run(
        [_NODE, "-e", script],
        capture_output=True,
        text=True,
        timeout=10.0,
        check=True,
    )
    return json.loads(result.stdout)


_NODE = shutil.which("node")


@unittest.skipUnless(_NODE, "node is not on PATH")
class DockerScriptSanitizerTests(unittest.TestCase):
    """Execute the embedded Node sanitizer for real, not a Python re-implementation.

    Each case feeds ``_run_docker_script`` a fake cache row and asserts on
    the emitted rows directly, then re-checks the same rows through
    ``pool_samples`` to prove the emitted shape actually drives the
    validation/failure semantics the caller relies on, not merely that the
    script ran without throwing.
    """

    def _rows_for_one_member(self, cache_json):
        return _run_docker_script([{"cache_json": cache_json}])

    def test_a_valid_current_cache_row_is_projected_for_every_window(self):
        cache_json = json.dumps(
            {
                "quotas": {
                    "session": {"remainingPercentage": 91.0,
                                "resetAt": "2026-08-24T19:20:58.435Z"},
                    "weekly": {"remainingPercentage": 42.5,
                               "resetAt": "2026-08-25T00:00:00Z"},
                    "mcp_monthly": {"remainingPercentage": 10.0, "resetAt": None},
                },
                "fetchedAt": "2026-08-24T19:17:02.435Z",
                "source": "scheduled",
            }
        )
        rows = self._rows_for_one_member(cache_json)
        by_window = {row["window_key"]: row for row in rows}
        self.assertEqual(set(by_window), {"session", "weekly", "mcp_monthly"})
        self.assertEqual(by_window["session"]["remaining_percentage"], 91.0)
        self.assertEqual(
            by_window["session"]["next_reset_at"], "2026-08-24T19:20:58.435Z"
        )
        self.assertEqual(
            by_window["session"]["fetched_at"], "2026-08-24T19:17:02.435Z"
        )
        self.assertIsNone(by_window["mcp_monthly"]["next_reset_at"])
        now = datetime.fromisoformat("2026-08-24T19:20:00Z").timestamp()
        samples = pool_samples(now, fetch_rows=lambda: rows)
        session = next(s for s in samples if s.window == "session_5h")
        self.assertEqual(session.remaining_percent, 91.0)

    def test_missing_empty_and_malformed_cache_all_poison_the_member(self):
        for cache_json in (None, "", "not json", json.dumps({"quotas": "not-an-object"})):
            with self.subTest(cache_json=cache_json):
                rows = self._rows_for_one_member(cache_json)
                self.assertEqual(len(rows), len(omniroute.WINDOWS))
                for row in rows:
                    self.assertIsNone(row["remaining_percentage"])
                    self.assertIsNone(row["next_reset_at"])
                    self.assertIsNone(row["fetched_at"])
                now = datetime.now(timezone.utc).timestamp()
                with self.assertRaises(CapacitySourceError) as raised:
                    pool_samples(now, fetch_rows=lambda rows=rows: rows)
                self.assertEqual(raised.exception.reason, "omniroute_malformed_data")

    def test_quotas_as_an_array_is_rejected_not_treated_as_an_object(self):
        cache_json = json.dumps(
            {"quotas": [{"remainingPercentage": 99.0}], "fetchedAt": "2026-08-24T19:17:02Z"}
        )
        rows = self._rows_for_one_member(cache_json)
        for row in rows:
            self.assertIsNone(row["remaining_percentage"])

    def test_a_reset_only_change_survives_the_real_node_sanitizer(self):
        earlier = json.dumps(
            {
                "quotas": {"session": {"remainingPercentage": 80.0,
                                        "resetAt": "2026-08-24T19:20:58.435Z"}},
                "fetchedAt": "2026-08-24T19:17:02.435Z",
            }
        )
        later = json.dumps(
            {
                "quotas": {"session": {"remainingPercentage": 80.0,
                                        "resetAt": "2026-08-24T20:20:58.435Z"}},
                "fetchedAt": "2026-08-24T19:17:02.435Z",
            }
        )
        now = datetime.fromisoformat("2026-08-24T19:17:02.435Z").timestamp() + 60.0

        def _session_only(cache_json):
            return [
                row for row in self._rows_for_one_member(cache_json)
                if row["window_key"] == "session"
            ]

        (before,) = pool_samples(now, fetch_rows=lambda: _session_only(earlier))
        (after,) = pool_samples(now, fetch_rows=lambda: _session_only(later))
        self.assertNotEqual(before.reset_at, after.reset_at)
        self.assertEqual(
            after.reset_at, datetime.fromisoformat("2026-08-24T20:20:58.435Z")
        )

    def test_sentinel_secrets_in_message_plan_and_connection_id_never_appear(self):
        secret = "SECRET-do-not-leak-me"
        cache_json = json.dumps(
            {
                "quotas": {"session": {"remainingPercentage": 50.0, "resetAt": None}},
                "fetchedAt": "2026-08-24T19:17:02Z",
                "message": secret,
                "plan": secret,
                "connectionId": secret,
                "source": "manual",
            }
        )
        rows = _run_docker_script([{"cache_json": cache_json, "connection_id": secret}])
        serialized = json.dumps(rows)
        self.assertNotIn(secret, serialized)

    def test_malformed_remaining_reset_and_fetched_are_never_emitted_raw(self):
        secret_object = {"leaked": "yes-this-should-never-cross-stdout"}
        cache_json = json.dumps(
            {
                "quotas": {
                    "session": {
                        "remainingPercentage": secret_object,
                        "resetAt": secret_object,
                    }
                },
                "fetchedAt": secret_object,
            }
        )
        rows = self._rows_for_one_member(cache_json)
        serialized = json.dumps(rows)
        self.assertNotIn("leaked", serialized)
        session_row = next(r for r in rows if r["window_key"] == "session")
        self.assertIsNone(session_row["remaining_percentage"])
        self.assertIsNone(session_row["fetched_at"])
        # A present-but-malformed reset must not silently become a
        # legitimate absent reset: it must still poison the window.
        self.assertIsNotNone(session_row["next_reset_at"])
        now = datetime.now(timezone.utc).timestamp()
        with self.assertRaises(CapacitySourceError) as raised:
            pool_samples(now, fetch_rows=lambda: rows)
        self.assertEqual(raised.exception.reason, "omniroute_malformed_data")

    def test_a_legitimately_absent_reset_stays_null_not_poisoned(self):
        cache_json = json.dumps(
            {
                "quotas": {"session": {"remainingPercentage": 77.0}},
                "fetchedAt": "2026-08-24T19:17:02Z",
            }
        )
        rows = self._rows_for_one_member(cache_json)
        session_row = next(r for r in rows if r["window_key"] == "session")
        self.assertIsNone(session_row["next_reset_at"])
        now = datetime.fromisoformat("2026-08-24T19:17:02Z").timestamp() + 60.0
        (sample,) = pool_samples(now, fetch_rows=lambda: [session_row])
        self.assertEqual(sample.remaining_percent, 77.0)
        self.assertIsNone(sample.reset_at)

    def test_emission_is_bounded_with_an_overflow_sentinel(self):
        members = [
            {
                "cache_json": json.dumps(
                    {
                        "quotas": {
                            "session": {"remainingPercentage": 1.0},
                            "weekly": {"remainingPercentage": 1.0},
                            "mcp_monthly": {"remainingPercentage": 1.0},
                        },
                        "fetchedAt": "2026-08-24T19:17:02Z",
                    }
                )
            }
            for _ in range(30)
        ]
        rows = _run_docker_script(members)
        self.assertLessEqual(len(rows), omniroute._MAX_EMITTED_ROWS + 1)
        self.assertGreater(len(rows), omniroute._MAX_EMITTED_ROWS)
        now = datetime.now(timezone.utc).timestamp()
        with self.assertRaises(CapacitySourceError) as raised:
            pool_samples(now, fetch_rows=lambda: rows)
        self.assertEqual(raised.exception.reason, "omniroute_result_overflow")


if __name__ == "__main__":
    unittest.main()
