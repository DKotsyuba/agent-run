"""Tests for exact-key capacity route snapshot construction."""

import tempfile
import unittest
from pathlib import Path

from agent_run.capacity.snapshot import build_capacity_routes
from agent_run.errors import ValidationError
from agent_run.state import StateStore


class CapacitySnapshotTests(unittest.TestCase):
    """Exercise exact joins, scope isolation, and cross-scope conflict gates."""

    def setUp(self) -> None:
        """Open one isolated v10 state store for a snapshot test."""

        self._directory = tempfile.TemporaryDirectory()
        self.store = StateStore.initialize(Path(self._directory.name) / "state.db")

    def tearDown(self) -> None:
        """Close the state store and remove its temporary directory."""

        self.store.close()
        self._directory.cleanup()

    def _append_scope(
        self,
        scope: str,
        *,
        runtime: str = "opaque-runtime",
        key_runtime: str | None = None,
        route_runtime: str | None = None,
        pool_id: str = "pool",
        route_id: str = "route",
        lane: str = "lane",
        account: str = "",
        samples: bool = True,
        sample_valid_until: float = 2_000.0,
        snapshot_valid_until: float = 1_100.0,
    ) -> None:
        """Persist one configurable sample/topology scope atomically.

        Runtime overrides deliberately create malformed persisted payloads for
        validation tests. Disabling samples leaves the exact topology key
        without history so missing-forecast behavior can be observed.
        """

        key_owner = runtime if key_runtime is None else key_runtime
        route_owner = runtime if route_runtime is None else route_runtime
        launch_account = account or f"account-{scope}"
        sample_rows = []
        if samples:
            sample_rows.append(
                {
                    "lane": lane,
                    "window": "window",
                    "source": "source",
                    "target": launch_account,
                    "remaining_percent": 50.0,
                    "reset_at": 2_000.0,
                    "observed_at": 900.0,
                    "valid_until": sample_valid_until,
                    "payload": None,
                }
            )
        self.store.append_capacity_samples(
            sample_rows,
            runtime=runtime,
            scope_id=scope,
            observed_at=900.0,
            valid_until=snapshot_valid_until,
            payload={
                "pools": [
                    {
                        "pool_id": pool_id,
                        "keys": [
                            {
                                "runtime": key_owner,
                                "lane": lane,
                                "window": "window",
                                "target": launch_account,
                                "source": "source",
                            }
                        ],
                    }
                ],
                "routes": [
                    {
                        "route_id": route_id,
                        "runtime": route_owner,
                        "account": launch_account,
                        "quota_lane": lane,
                        "pool_ids": [pool_id],
                    }
                ],
            },
        )

    def test_fresh_snapshot_joins_exact_key_forecast(self) -> None:
        """A fresh arbitrary route is routable only with its exact history key."""

        self._append_scope("fresh")
        snapshot = build_capacity_routes(self.store, retention=10, now=1_000.0)
        self.assertEqual(len(snapshot.routes), 1)
        self.assertEqual(
            snapshot.routes[0].forecasts[0].key.target, "account-fresh"
        )

    def test_identical_cross_scope_definitions_collapse(self) -> None:
        """Identical global pool and route ids across scopes produce one route."""

        self._append_scope("one", account="shared-account")
        self._append_scope("two", account="shared-account")
        snapshot = build_capacity_routes(self.store, retention=20, now=1_000.0)
        self.assertEqual(len(snapshot.routes), 1)
        self.assertEqual(snapshot.routes[0].descriptor.route_id, "route")
        self.assertFalse(snapshot.deferred)

    def test_conflicting_pool_and_route_definitions_remove_every_route(self) -> None:
        """A later conflict removes both earlier and later affected definitions."""

        self._append_scope("pool-a", pool_id="shared", route_id="route-a", lane="a")
        self._append_scope("pool-b", pool_id="shared", route_id="route-b", lane="b")
        self._append_scope(
            "route-a",
            runtime="route-runtime",
            pool_id="pool-a",
            route_id="shared-route",
            lane="a",
        )
        self._append_scope(
            "route-b",
            runtime="route-runtime",
            pool_id="pool-b",
            route_id="shared-route",
            lane="b",
        )
        snapshot = build_capacity_routes(self.store, retention=20, now=1_000.0)
        self.assertFalse(snapshot.routes)
        self.assertEqual({item.reason for item in snapshot.deferred}, {"conflict"})
        self.assertEqual(
            {item.scope_id for item in snapshot.deferred},
            {"pool-a", "pool-b", "route-a", "route-b"},
        )

    def test_bad_scopes_do_not_hide_an_independent_fresh_scope(self) -> None:
        """Expired, malformed, and cross-runtime scopes remain isolated evidence."""

        self._append_scope("good", runtime="runtime-good")
        self._append_scope(
            "expired", runtime="runtime-old", snapshot_valid_until=950.0
        )
        self._append_scope(
            "cross",
            runtime="runtime-cross",
            key_runtime="other-runtime",
            route_runtime="other-runtime",
            samples=False,
        )
        self._append_scope("broken", runtime="runtime-bad", samples=False)
        with self.store.connection:
            self.store.connection.execute(
                """
                UPDATE capacity_route_snapshots
                SET payload_json = ?
                WHERE runtime = ? AND scope_id = ?
                """,
                ("{", "runtime-bad", "broken"),
            )
        snapshot = build_capacity_routes(self.store, retention=20, now=1_000.0)
        self.assertEqual(
            [route.descriptor.runtime for route in snapshot.routes],
            ["runtime-good"],
        )
        self.assertEqual(
            {(item.runtime, item.reason) for item in snapshot.unavailable},
            {("runtime-old", "expired")},
        )
        self.assertEqual(
            {(item.runtime, item.reason) for item in snapshot.deferred},
            {
                ("runtime-bad", "malformed"),
                ("runtime-cross", "malformed"),
            },
        )

    def test_missing_unknown_and_legacy_evidence_never_become_routes(self) -> None:
        """Missing/expired forecasts defer while legacy samples infer no topology."""

        self._append_scope("missing", runtime="runtime-missing", samples=False)
        self._append_scope(
            "unknown",
            runtime="runtime-unknown",
            sample_valid_until=950.0,
        )
        self.store.insert_capacity_sample(
            runtime="legacy-runtime",
            lane="legacy",
            window="window",
            source="legacy-source",
            target="legacy-account",
            remaining_percent=99.0,
            reset_at=2_000.0,
            observed_at=900.0,
            valid_until=2_000.0,
            payload={},
        )
        snapshot = build_capacity_routes(self.store, retention=20, now=1_000.0)
        self.assertFalse(snapshot.routes)
        self.assertEqual(
            {(item.runtime, item.reason) for item in snapshot.deferred},
            {("runtime-missing", "missing_forecast")},
        )
        self.assertEqual(
            {(item.runtime, item.reason) for item in snapshot.unavailable},
            {("runtime-unknown", "unknown_forecast")},
        )
        self.assertNotIn(
            "legacy-runtime",
            {item.runtime for item in (*snapshot.deferred, *snapshot.unavailable)},
        )

    def test_invalid_inputs_raise_validation_error(self) -> None:
        """Non-positive retention and non-finite time are rejected."""

        with self.assertRaises(ValidationError):
            build_capacity_routes(self.store, retention=0, now=1.0)
        with self.assertRaises(ValidationError):
            build_capacity_routes(self.store, retention=1, now=float("nan"))
