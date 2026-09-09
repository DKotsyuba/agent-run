import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from agent_run.adapters.base import LimitSample
from agent_run.capacity import sources
from agent_run.capacity.collect import collect_slice
from agent_run.capacity.history import CapacityKey
from agent_run.capacity.topology import (
    CapacityRouteDescriptor,
    CapacityTopology,
    PhysicalPoolDescriptor,
    pools_from_samples,
    validate_slice,
    validate_topology,
)
from agent_run.config import CapacityConfig, RuntimeConfig
from agent_run.errors import ValidationError


def _runtime_config(**overrides) -> RuntimeConfig:
    defaults = dict(
        enabled=True,
        adapter="fake:ADAPTER",
        binary=Path("/usr/bin/fake"),
        home=Path("/tmp/fake-home"),
        models=("model-a",),
    )
    defaults.update(overrides)
    return RuntimeConfig(**defaults)


def _sample(lane, window, target=None, source="native", valid_for_seconds=900):
    return LimitSample(
        lane=lane,
        window=window,
        remaining_percent=50.0,
        reset_at=None,
        observed_at=None,
        source=source,
        target=target,
        valid_for_seconds=valid_for_seconds,
    )


# Fictitious names throughout: no real provider account appears here.
_RUNTIME = "fictitious"
_SHARED = _sample("primary", "five_hour")
_SCOPED_A = _sample("secondary", "seven_day", target="scoped-alpha")
_SCOPED_B = _sample("secondary", "seven_day", target="scoped-beta")


class TopologyValidationTests(unittest.TestCase):
    def test_invalid_pool_reference_rejects_the_whole_topology(self) -> None:
        pool = PhysicalPoolDescriptor(
            "pool-1", frozenset({CapacityKey(_RUNTIME, "primary", "five_hour", None, "native")})
        )
        route = CapacityRouteDescriptor(
            "route-1", _RUNTIME, None, "primary", ("missing-pool",)
        )
        with self.assertRaises(ValidationError):
            validate_topology((pool,), (route,))

    def test_validation_is_independent_of_input_order(self) -> None:
        keys_a = frozenset({CapacityKey(_RUNTIME, "primary", "five_hour", None, "native")})
        keys_b = frozenset({CapacityKey(_RUNTIME, "secondary", "seven_day", None, "native")})
        pools = (
            PhysicalPoolDescriptor("pool-a", keys_a),
            PhysicalPoolDescriptor("pool-b", keys_b),
        )
        routes = (
            CapacityRouteDescriptor("route-a", _RUNTIME, None, "primary", ("pool-a",)),
            CapacityRouteDescriptor("route-b", _RUNTIME, "team1", "secondary", ("pool-b",)),
        )
        forward = validate_topology(pools, routes)
        backward = validate_topology(
            tuple(reversed(pools)), tuple(reversed(routes))
        )
        self.assertEqual(forward, backward)
        self.assertEqual(
            [pool.pool_id for pool in forward.pools], ["pool-a", "pool-b"]
        )
        self.assertEqual(
            [route.route_id for route in forward.routes], ["route-a", "route-b"]
        )

    def test_shared_pool_is_referenced_without_multiplying_and_distinct_stay_distinct(
        self,
    ) -> None:
        shared = PhysicalPoolDescriptor(
            "pool-shared",
            frozenset({CapacityKey(_RUNTIME, "primary", "five_hour", None, "native")}),
        )
        first = PhysicalPoolDescriptor(
            "pool-first",
            frozenset({CapacityKey(_RUNTIME, "primary", "five_hour", "team1", "native")}),
        )
        second = PhysicalPoolDescriptor(
            "pool-second",
            frozenset({CapacityKey(_RUNTIME, "primary", "five_hour", "team2", "native")}),
        )
        topology = validate_topology(
            (shared, first, second),
            (
                CapacityRouteDescriptor("r-default", _RUNTIME, None, "primary", ("pool-shared",)),
                CapacityRouteDescriptor("r-team1", _RUNTIME, "team1", "primary", ("pool-shared", "pool-first")),
                CapacityRouteDescriptor("r-team2", _RUNTIME, "team2", "primary", ("pool-shared", "pool-second")),
            ),
        )
        references = [pool_id for route in topology.routes for pool_id in route.pool_ids]
        # One shared reservoir referenced by three routes, two distinct pools
        # kept apart: capacity is stated once per physical pool, never per route.
        self.assertEqual(references.count("pool-shared"), 3)
        self.assertEqual(len(topology.pools), 3)
        self.assertNotIn("pool-first", topology.routes[1].pool_ids[:1])

    def test_empty_key_set_duplicate_ids_and_duplicate_references_reject(self) -> None:
        good = PhysicalPoolDescriptor(
            "pool-1", frozenset({CapacityKey(_RUNTIME, "primary", "five_hour", None, "native")})
        )
        with self.assertRaises(ValidationError):
            validate_topology((PhysicalPoolDescriptor("pool-2", frozenset()),), ())
        with self.assertRaises(ValidationError):
            validate_topology((good, good), ())
        route = CapacityRouteDescriptor("route-1", _RUNTIME, None, "primary", ("pool-1", "pool-1"))
        with self.assertRaises(ValidationError):
            validate_topology((good,), (route,))

    def test_slice_validates_everything_before_persistence(self) -> None:
        topology = validate_topology(
            pools_from_samples(_RUNTIME, (_SHARED,)), ()
        )
        slice_ok = validate_slice(
            _RUNTIME, "scope:default", (_SHARED,), topology, 100.0, 200.0
        )
        self.assertEqual(slice_ok.samples, (_SHARED,))
        self.assertEqual(slice_ok.runtime, _RUNTIME)
        self.assertEqual(slice_ok.scope_id, "scope:default")
        for bad in (
            ("", "scope:default", (), topology, 100.0, 200.0),
            (_RUNTIME, "scope:default", ("not-a-sample",), topology, 100.0, 200.0),
            (_RUNTIME, "scope:default", (), "not-a-topology", 100.0, 200.0),
            (_RUNTIME, "scope:default", (), topology, 200.0, 100.0),
            (_RUNTIME, "scope:default", (), topology, 100.0, 200.0),
            (_RUNTIME, "scope:default", (), topology, float("nan"), 200.0),
        ):
            with self.subTest(bad=bad[1:3]):
                self.assertRaises(ValidationError, validate_slice, *bad)


class SourceTopologyTests(unittest.TestCase):
    def test_native_composes_shared_and_scoped_lanes(self) -> None:
        topology = sources.sample_topology(
            "clara-runtime", _runtime_config(limits_source="native"),
            (_SHARED, _SCOPED_A, _SCOPED_B),
        )
        routes = {route.quota_lane: route for route in topology.routes}
        self.assertEqual(set(routes), {"default", "scoped-alpha", "scoped-beta"})
        self.assertEqual({route.account for route in routes.values()}, {None})
        self.assertEqual(len(topology.pools), 3)
        shared = next(pool.pool_id for pool in topology.pools if next(iter(pool.keys)).target is None)
        self.assertEqual(routes["default"].pool_ids, (shared,))
        for scope in ("scoped-alpha", "scoped-beta"):
            self.assertIn(shared, routes[scope].pool_ids)
            self.assertEqual(len(routes[scope].pool_ids), 2)

    def test_native_topology_ignores_sample_input_order(self) -> None:
        forward = sources.sample_topology(
            "clara-runtime", _runtime_config(), (_SHARED, _SCOPED_A)
        )
        backward = sources.sample_topology(
            "clara-runtime", _runtime_config(), (_SCOPED_A, _SHARED)
        )
        self.assertEqual(forward, backward)

    def test_codexbar_routes_only_configured_accounts(self) -> None:
        samples = (
            _sample("primary", "five_hour", source="codexbar"),
            _sample("primary", "five_hour", target="team1", source="codexbar"),
            _sample("secondary", "weekly", target="team1", source="codexbar"),
            _sample("primary", "five_hour", target="team2", source="codexbar"),
            _sample("primary", "five_hour", target="stranger@example.test", source="codexbar"),
        )
        runtime = _runtime_config(
            limits_source="codexbar", accounts=("team1", "team2")
        )
        topology = sources.sample_topology("cortex-runtime", runtime, samples)
        accounts = {route.account for route in topology.routes}
        self.assertEqual(accounts, {None, "team1", "team2"})
        self.assertEqual({route.quota_lane for route in topology.routes}, {"default"})
        team1 = next(route for route in topology.routes if route.account == "team1")
        self.assertEqual(len(team1.pool_ids), 2)
        # The unknown email keeps its pool and sample but no route.
        self.assertEqual(len(topology.pools), 5)
        self.assertEqual(len(topology.routes), 3)
        self.assertFalse(
            any("stranger" in route.route_id for route in topology.routes)
        )

    def test_omniroute_builds_one_aggregate_route(self) -> None:
        samples = (
            _sample("pool", "session_5h", source="omniroute_quota_pool"),
            _sample("pool", "weekly", source="omniroute_quota_pool"),
        )
        topology = sources.sample_topology(
            "orion-runtime", _runtime_config(limits_source="omniroute"), samples
        )
        (route,) = topology.routes
        self.assertEqual(route.route_id, "orion-runtime:aggregate")
        self.assertEqual(route.quota_lane, "aggregate")
        self.assertIsNone(route.account)
        self.assertEqual(tuple(route.pool_ids), tuple(p.pool_id for p in topology.pools))
        self.assertEqual(len(topology.pools), 2)

    def test_legacy_samples_and_collect_samples_are_unchanged(self) -> None:
        # The neutral grouping never rewrites the samples it is given, and the
        # collect_samples reports an explicit none source as unsupported.
        samples = (_SHARED, _SCOPED_A)
        topology = sources.sample_topology("clara-runtime", _runtime_config(), samples)
        self.assertEqual(samples, (_SHARED, _SCOPED_A))
        self.assertIsInstance(topology, CapacityTopology)
        self.assertIsNone(
            sources.collect_samples(
                "clara-runtime", _runtime_config(limits_source="none"), CapacityConfig(), None
            )
        )


class CollectSliceTests(unittest.TestCase):
    def test_collect_slice_validates_whole_slice_and_reports_shelf_life(self) -> None:
        from datetime import datetime, timezone

        observed = datetime(2026, 9, 2, 12, 0, tzinfo=timezone.utc)
        sample = LimitSample(
            lane="primary",
            window="five_hour",
            remaining_percent=40.0,
            reset_at=None,
            observed_at=observed,
            source="codexbar",
            target="team1",
            valid_for_seconds=600,
        )
        shorter = LimitSample(
            lane="secondary",
            window="weekly",
            remaining_percent=40.0,
            reset_at=None,
            observed_at=observed,
            source="codexbar",
            target="team1",
            valid_for_seconds=300,
        )

        def load(_name, _config):
            raise AssertionError("codexbar never loads an adapter")

        runtime = _runtime_config(limits_source="codexbar", accounts=("team1",))

        def fake_samples(name, config, capacity, loader, home=None):
            self.assertIs(loader, load)
            return (sample, shorter)

        original = sources.collect_samples
        sources.collect_samples = fake_samples
        try:
            result = collect_slice(
                "cortex-runtime", runtime, CapacityConfig(), load, at=1000.0
            )
        finally:
            sources.collect_samples = original
        self.assertIsNotNone(result)
        self.assertEqual(result.scope_id, "cortex-runtime")
        self.assertEqual(result.samples, (sample, shorter))
        self.assertEqual(result.observed_at, observed.timestamp())
        self.assertEqual(result.valid_until, observed.timestamp() + 300)
        self.assertEqual(
            {route.account for route in result.topology.routes}, {"team1"}
        )

    def test_collect_slice_passes_none_through_for_unsupported_sources(self) -> None:
        class NoLimits:
            def describe(self):
                class Info:
                    name = "cortex-runtime"
                    adapter_api_version = 1
                    capabilities = frozenset()

                return Info()

            def validate(self, config):
                return None

        self.assertIsNone(
            collect_slice(
                "cortex-runtime",
                _runtime_config(),
                CapacityConfig(),
                lambda name, config: NoLimits(),
            )
        )

    def test_persist_slice_consumes_the_state_api_atomically(self) -> None:
        import json
        import tempfile

        from agent_run.capacity.collect import persist_slice
        from agent_run.state import StateStore

        sample = LimitSample(
            lane="primary",
            window="five_hour",
            remaining_percent=30.0,
            reset_at=None,
            observed_at=None,
            source="codexbar",
            target="team1",
            valid_for_seconds=600,
        )
        collected = validate_slice(
            "cortex-runtime",
            "cortex-runtime",
            (sample,),
            sources.sample_topology(
                "cortex-runtime",
                _runtime_config(limits_source="codexbar", accounts=("team1",)),
                (sample,),
            ),
            1000.0,
            1600.0,
        )
        with tempfile.TemporaryDirectory() as directory:
            store = StateStore.initialize(Path(directory) / "state.db")
            try:
                persist_slice(store, collected)
                rows = store.capacity_sample_history(retention=10, runtime="cortex-runtime")
                snapshots = store.capacity_route_snapshots(runtime="cortex-runtime")
            finally:
                store.close()
        (row,) = rows
        self.assertEqual(row["lane"], "primary")
        self.assertEqual(row["target"], "team1")
        self.assertEqual(row["observed_at"], 1000.0)
        self.assertEqual(row["valid_until"], 1600.0)
        (snapshot,) = snapshots
        self.assertEqual(snapshot["scope_id"], "cortex-runtime")
        payload = json.loads(snapshot["payload_json"])
        self.assertEqual(payload["routes"][0]["account"], "team1")
        self.assertTrue(payload["pools"])

    def test_same_runtime_keeps_distinct_scopes_and_key_runtimes(self) -> None:
        """Separate opaque scopes persist independently under one runtime."""
        import json
        import tempfile

        from agent_run.capacity.collect import persist_slice
        from agent_run.state import StateStore

        sample = _sample("primary", "five_hour", target="team1")
        topology = validate_topology(pools_from_samples(_RUNTIME, (sample,)), ())
        first = validate_slice(
            _RUNTIME, "account:personal2", (sample,), topology, 1.0, 2.0
        )
        second = validate_slice(_RUNTIME, "account:work", (sample,), topology, 3.0, 4.0)
        self.assertEqual(
            {key.runtime for pool in first.topology.pools for key in pool.keys},
            {_RUNTIME},
        )

        with tempfile.TemporaryDirectory() as directory:
            store = StateStore.initialize(Path(directory) / "state.db")
            try:
                persist_slice(store, first)
                persist_slice(store, second)
                snapshots = store.capacity_route_snapshots(runtime=_RUNTIME)
            finally:
                store.close()

        self.assertEqual(
            {snapshot["scope_id"] for snapshot in snapshots},
            {"account:personal2", "account:work"},
        )
        for snapshot in snapshots:
            payload = json.loads(snapshot["payload_json"])
            self.assertEqual(payload["pools"][0]["keys"][0]["runtime"], _RUNTIME)


if __name__ == "__main__":
    unittest.main()
