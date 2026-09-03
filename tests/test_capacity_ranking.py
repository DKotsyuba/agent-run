"""Contract tests for provider-neutral capacity route ordering."""

from __future__ import annotations

from collections.abc import Mapping
from typing import cast
import unittest

from agent_run.capacity.forecast import CapacityForecast
from agent_run.capacity.history import CapacityKey
from agent_run.capacity.ranking import rank_capacity_routes
from agent_run.capacity.snapshot import (
    CapacityRoute,
    CapacityRouteEvidence,
    CapacityRouteSnapshot,
)
from agent_run.capacity.topology import (
    CapacityRouteDescriptor,
    PhysicalPoolDescriptor,
)
from agent_run.errors import ValidationError


#: Shared injected epoch used by every deterministic ranking test.
_NOW = 1_000.0


def _forecast(
    runtime: str,
    lane: str,
    remaining: float,
    *,
    reset_at: float | None = 4_600.0,
    burn: float | None = None,
    span: float | None = None,
    warmup: bool | None = None,
    risk: str = "low",
    known: bool = True,
    observed_at: float | None = _NOW,
) -> CapacityForecast:
    """Build one exact forecast with explicit burn evidence for a test route.

    ``runtime`` and ``lane`` form an opaque key. Percentages and timestamps use
    the same domains as production forecasts; ``warmup`` defaults to whether
    burn is absent. Unknown forecasts intentionally carry no remaining value.
    """

    key = CapacityKey(runtime, lane, "window", None, "source")
    return CapacityForecast(
        key=key,
        known=known,
        remaining_percent=remaining if known else None,
        reset_at=reset_at if known else None,
        observed_at=observed_at if known else None,
        warmup=(burn is None) if warmup is None else warmup,
        burn_percent_per_hour=burn if known else None,
        sustainable_percent_per_hour=None,
        risk=risk if known else "unknown",
        burn_span_seconds=span if known else None,
    )


def _route(
    runtime: str,
    route_id: str,
    pool_id: str,
    forecasts: tuple[CapacityForecast, ...],
    *,
    account: str | None = None,
    quota_lane: str = "lane",
) -> CapacityRoute:
    """Build one route whose single physical pool governs ``forecasts``.

    Identical ``runtime`` and ``pool_id`` values let tests model concrete route
    aliases without inventing provider-specific names.
    """

    pool = PhysicalPoolDescriptor(
        pool_id, frozenset(forecast.key for forecast in forecasts)
    )
    descriptor = CapacityRouteDescriptor(
        route_id, runtime, account, quota_lane, (pool_id,)
    )
    return CapacityRoute(descriptor, (pool,), forecasts)


def _snapshot(
    *routes: CapacityRoute,
    deferred: tuple[CapacityRouteEvidence, ...] = (),
    unavailable: tuple[CapacityRouteEvidence, ...] = (),
) -> CapacityRouteSnapshot:
    """Build an immutable snapshot with optional pre-ranking evidence."""

    return CapacityRouteSnapshot(tuple(routes), deferred, unavailable)


class CapacityRankingTests(unittest.TestCase):
    """Verify gates, scoring, multipliers, aliases, and total ordering."""

    def test_reliable_projection_and_multiplier_determine_priority(self) -> None:
        """Reliable burn projects to reset before applying the runtime factor."""

        route_a = _route(
            "runtime-a",
            "route-a",
            "pool-a",
            (_forecast("runtime-a", "lane-a", 80.0, burn=20.0, span=7_200.0),),
        )
        route_b = _route(
            "runtime-b",
            "route-b",
            "pool-b",
            (_forecast("runtime-b", "lane-b", 90.0, burn=0.0, span=7_200.0),),
        )
        order = rank_capacity_routes(
            _snapshot(route_b, route_a), {"runtime-a": 2.0}, now=_NOW
        )
        first = order.routes[0]
        self.assertEqual(first.runtime, "runtime-a")
        self.assertAlmostEqual(first.score, 1.6)
        self.assertEqual(first.multiplier, 2.0)
        self.assertAlmostEqual(first.priority, 3.2)
        self.assertEqual(first.windows[0].projected_percent, 60.0)
        self.assertEqual(first.windows[0].marker, "projected")
        self.assertFalse(order.insufficient_diversity)

    def test_fallback_markers_use_centered_remaining_percent(self) -> None:
        """Warmup, thin, and reset-less windows never extrapolate thin burn."""

        route = _route(
            "opaque-runtime",
            "route",
            "pool",
            (
                _forecast("opaque-runtime", "warm", 75.0),
                _forecast(
                    "opaque-runtime", "thin", 75.0, burn=5.0, span=1_800.0
                ),
                _forecast(
                    "opaque-runtime",
                    "none",
                    75.0,
                    reset_at=None,
                    burn=5.0,
                    span=7_200.0,
                ),
            ),
        )
        order = rank_capacity_routes(_snapshot(route), {}, now=_NOW)
        self.assertAlmostEqual(order.routes[0].score, 1.5)
        self.assertEqual(
            {item.marker for item in order.routes[0].windows},
            {"warmup", "thin_evidence", "no_reset"},
        )
        self.assertTrue(
            all(item.projected_percent is None for item in order.routes[0].windows)
        )

    def test_exhaustion_is_omitted_and_multiplier_cannot_revive_zero_score(self) -> None:
        """A zero window is omitted while forecast-zero priority stays last."""

        exhausted = _forecast("provider-x", "empty", 0.0)
        alias_a = _route(
            "provider-x", "route-a", "shared-pool", (exhausted,), account="a"
        )
        alias_b = _route(
            "provider-x", "route-b", "shared-pool", (exhausted,), account="b"
        )
        high = _route(
            "provider-y",
            "high",
            "high-pool",
            (_forecast("provider-y", "high", 1.0, risk="high"),),
        )
        zero_score = _route(
            "provider-z",
            "zero",
            "zero-pool",
            (
                _forecast(
                    "provider-z", "zero", 1.0, burn=200.0, span=7_200.0
                ),
            ),
        )
        order = rank_capacity_routes(
            _snapshot(alias_b, zero_score, high, alias_a),
            {"provider-z": 100.0},
            now=_NOW,
        )
        self.assertEqual(len(order.omitted), 1)
        self.assertEqual(
            [alias.account for alias in order.omitted[0].aliases], ["a", "b"]
        )
        self.assertEqual([item.runtime for item in order.routes], ["provider-y", "provider-z"])
        self.assertEqual(order.routes[0].windows[0].risk, "high")
        self.assertEqual(order.routes[1].score, 0.0)
        self.assertEqual(order.routes[1].priority, 0.0)

    def test_alias_collapse_preserves_concrete_launch_descriptors(self) -> None:
        """Shared physical capacity appears once with every account alias."""

        forecast = _forecast("provider-ζ", "shared", 80.0)
        routes = (
            _route(
                "provider-ζ",
                "route-b",
                "pool",
                (forecast,),
                account="account-b",
                quota_lane="lane-b",
            ),
            _route(
                "provider-ζ",
                "route-a",
                "pool",
                (forecast,),
                account="account-a",
                quota_lane="lane-a",
            ),
        )
        (ranked,) = rank_capacity_routes(_snapshot(*routes), {}, now=_NOW).routes
        self.assertEqual(ranked.pool_ids, ("pool",))
        self.assertEqual(
            [(alias.route_id, alias.account, alias.quota_lane) for alias in ranked.aliases],
            [
                ("route-a", "account-a", "lane-a"),
                ("route-b", "account-b", "lane-b"),
            ],
        )
        self.assertTrue(
            rank_capacity_routes(_snapshot(*routes), {}, now=_NOW).insufficient_diversity
        )

    def test_tie_break_is_total_and_input_order_independent(self) -> None:
        """Limiting reset and route id settle equal score permutations."""

        routes = (
            _route(
                "runtime", "route-c", "pool-c", (_forecast("runtime", "c", 60.0, reset_at=3_000.0),)
            ),
            _route(
                "runtime", "route-b", "pool-b", (_forecast("runtime", "b", 60.0, reset_at=2_000.0),)
            ),
            _route(
                "runtime", "route-a", "pool-a", (_forecast("runtime", "a", 60.0, reset_at=2_000.0),)
            ),
        )
        forward = rank_capacity_routes(_snapshot(*routes), {}, now=_NOW)
        reverse = rank_capacity_routes(_snapshot(*reversed(routes)), {}, now=_NOW)
        self.assertEqual(forward, reverse)
        self.assertEqual(
            [item.aliases[0].route_id for item in forward.routes],
            ["route-a", "route-b", "route-c"],
        )

    def test_new_forecast_snapshot_can_change_order(self) -> None:
        """Re-ranking current evidence deterministically reverses preference."""

        first = rank_capacity_routes(
            _snapshot(
                _route("runtime", "a", "pool-a", (_forecast("runtime", "a", 20.0),)),
                _route("runtime", "b", "pool-b", (_forecast("runtime", "b", 80.0),)),
            ),
            {},
            now=_NOW,
        )
        second = rank_capacity_routes(
            _snapshot(
                _route("runtime", "a", "pool-a", (_forecast("runtime", "a", 90.0),)),
                _route("runtime", "b", "pool-b", (_forecast("runtime", "b", 10.0),)),
            ),
            {},
            now=_NOW,
        )
        self.assertEqual(first.routes[0].aliases[0].route_id, "b")
        self.assertEqual(second.routes[0].aliases[0].route_id, "a")

    def test_deferred_evidence_and_unavailable_runtime_are_complete(self) -> None:
        """Snapshot and ranker deferrals remain visible without hiding siblings."""

        good = _route(
            "runtime-u", "good", "good-pool", (_forecast("runtime-u", "good", 50.0),)
        )
        unknown = _route(
            "runtime-x",
            "unknown",
            "unknown-pool",
            (_forecast("runtime-x", "unknown", 0.0, known=False),),
        )
        snapshot = _snapshot(
            unknown,
            good,
            deferred=(CapacityRouteEvidence("runtime-d", "scope-d", "malformed", "bad"),),
            unavailable=(CapacityRouteEvidence("runtime-u", "scope-old", "expired", "old"),),
        )
        order = rank_capacity_routes(snapshot, {}, now=_NOW)
        self.assertEqual(
            {item.reason for item in order.deferred},
            {"malformed", "expired", "ranker_unknown_forecast"},
        )
        self.assertEqual(order.unavailable_runtimes, ("runtime-d", "runtime-x"))
        self.assertEqual(order.routes[0].runtime, "runtime-u")

    def test_invalid_arguments_raise_validation_error(self) -> None:
        """The pure API rejects malformed snapshots, time, and multipliers."""

        snapshot = _snapshot()
        invalid_calls = (
            lambda: rank_capacity_routes(
                cast(CapacityRouteSnapshot, object()), {}, now=_NOW
            ),
            lambda: rank_capacity_routes(
                snapshot, cast(Mapping[str, float], object()), now=_NOW
            ),
            lambda: rank_capacity_routes(snapshot, {}, now=cast(float, True)),
            lambda: rank_capacity_routes(snapshot, {}, now=-1.0),
            lambda: rank_capacity_routes(snapshot, {"": 1.0}, now=_NOW),
            lambda: rank_capacity_routes(snapshot, {"runtime": 0.0}, now=_NOW),
            lambda: rank_capacity_routes(
                snapshot, {"runtime": cast(float, True)}, now=_NOW
            ),
            lambda: rank_capacity_routes(snapshot, {"runtime": float("nan")}, now=_NOW),
        )
        for call in invalid_calls:
            with self.subTest(call=repr(call)):
                with self.assertRaises(ValidationError):
                    call()

    def test_route_multiplier_is_scoped_by_runtime_and_alias_weight_is_maximum(self) -> None:
        """Same route ids stay isolated while aliases choose the largest factor."""

        shared_a = _route("provider-a", "shared", "pool-a", (_forecast("provider-a", "a", 80.0),))
        alias_a = _route("provider-a", "alias", "pool-a", (_forecast("provider-a", "a", 80.0),))
        shared_b = _route("provider-b", "shared", "pool-b", (_forecast("provider-b", "b", 80.0),))
        order = rank_capacity_routes(
            _snapshot(shared_b, alias_a, shared_a),
            {"provider-a": 1.0, "provider-b": 1.0},
            now=_NOW,
            route_multipliers={("provider-a", "shared"): 3.0, ("provider-a", "alias"): 2.0, ("provider-b", "shared"): 0.5},
        )
        self.assertEqual(order.routes[0].runtime, "provider-a")
        self.assertEqual(order.routes[0].aliases[0].route_id, "shared")
        self.assertEqual(order.routes[0].multiplier, 3.0)
        self.assertEqual(order.routes[1].multiplier, 0.5)

    def test_route_multiplier_keys_and_values_are_strictly_validated(self) -> None:
        """Reject non-pair keys, blank parts, booleans, and nonpositive/nonfinite factors."""

        invalid = (
            {"route": 1.0},
            {("runtime",): 1.0},
            {("", "route"): 1.0},
            {("runtime", ""): 1.0},
            {("runtime", "route"): True},
            {("runtime", "route"): 0.0},
            {("runtime", "route"): -1.0},
            {("runtime", "route"): float("nan")},
            {("runtime", "route"): float("inf")},
        )
        for mapping in invalid:
            with self.subTest(mapping=mapping), self.assertRaises(ValidationError):
                rank_capacity_routes(
                    _snapshot(), {}, now=_NOW,
                    route_multipliers=cast(Mapping[tuple[str, str], float], mapping),
                )


if __name__ == "__main__":
    unittest.main()
