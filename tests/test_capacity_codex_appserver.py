"""Regression coverage for the Codex app-server capacity source.

Fictitious limit ids, account labels, and backend account ids throughout: no
real provider account appears here.  The fixtures encode the shapes captured
from ``codex app-server``'s ``account/rateLimits/read``: multi-bucket
``rateLimitsByLimitId`` preferred, legacy single-bucket ``rateLimits`` wrapped,
and JSON-RPC-style wrapped results accepted for captured fixtures.
"""

import sys
import time
import unittest
from datetime import datetime, timezone
from pathlib import Path
from typing import cast
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from agent_run.accounts import account_runtime_home
from agent_run.adapters.base import LaunchPlan
from agent_run.adapters.codex import app_server
from agent_run.adapters.codex import rate_limits as codex_rate_limits
from agent_run.capacity import sources
from agent_run.capacity.codex_appserver import normalize_rate_limits
from agent_run.capacity.collect import persist_slice
from agent_run.capacity.topology import CapacityCollectionSlice
from agent_run.config import RuntimeConfig
from agent_run.errors import ValidationError

# Runtime name used by normalized sample identities.
_RUNTIME = "fictitious"
# Provider source label expected on normalized samples.
_SOURCE = "codex_appserver"
# Fixed collection epoch shared by deterministic fixtures.
_OBSERVED = 1_785_000_000.0
# Near reset used to verify reset distance is not freshness.
_RESET_SOON = _OBSERVED + 3_600.0
# Distant reset used to verify bounded freshness.
_RESET_DAYS_AWAY = _OBSERVED + 5 * 24 * 3_600.0


def _runtime_config(*, accounts: tuple[str, ...] = ()) -> RuntimeConfig:
    """Build a minimal runtime configuration for optional account labels."""

    return RuntimeConfig(
        enabled=True,
        adapter="fake:ADAPTER",
        binary=Path("/usr/bin/fake-codex"),
        home=Path("/tmp/fictitious-home"),
        models=("model-a",),
        accounts=accounts,
    )


def _window(
    used: object, minutes: object, resets: float | None = _RESET_SOON
) -> dict[str, object]:
    """Build one app-server rate-limit window fixture."""
    window = {"usedPercent": used, "windowDurationMins": minutes}
    if resets is not None:
        window["resetsAt"] = resets
    return window


def _pro_response(account_id: str = "acct-pro-1") -> dict[str, object]:
    """Base Pro home: standard weekly plus Spark 5h/weekly buckets."""

    return {
        "accountId": account_id,
        "rateLimitsByLimitId": {
            "codex_standard_weekly": {
                "limitName": "Standard",
                "secondary": _window(20.0, 10080),
            },
            "codex_spark": {
                "limitName": "Spark",
                "primary": _window(10.0, 300),
                "secondary": _window(30.0, 10080, resets=None),
            },
        },
    }


def _plus_response(account_id: str = "acct-plus-1") -> dict[str, object]:
    """Plus account home: standard 5h/weekly, no display name on the bucket."""

    return {
        "accountId": account_id,
        "rateLimitsByLimitId": {
            "codex_plus": {
                "primary": _window(40.0, 300),
                "secondary": _window(60.0, 10080),
            },
        },
    }


class NormalizeRateLimitsTests(unittest.TestCase):
    """Verify direct, wrapped, legacy, and malformed response normalization."""
    def test_pro_standard_and_spark_are_distinct_routes_over_every_valid_window(self):
        """Ensure standard and Spark buckets retain distinct stable routes."""
        samples, topology, account_id = normalize_rate_limits(
            _RUNTIME, None, _pro_response(), _OBSERVED
        )
        self.assertEqual(account_id, "acct-pro-1")
        observed = datetime.fromtimestamp(_OBSERVED, timezone.utc)
        by_identity = {(s.lane, s.window): s for s in samples}
        self.assertEqual(
            set(by_identity),
            {
                ("codex_standard_weekly", "seven_day"),
                ("codex_spark", "five_hour"),
                ("codex_spark", "seven_day"),
            },
        )
        for sample in samples:
            self.assertEqual(sample.source, _SOURCE)
            self.assertIsNone(sample.target)
            self.assertEqual(sample.observed_at, observed)
            self.assertEqual(sample.valid_for_seconds, 900)
        self.assertEqual(
            by_identity[("codex_standard_weekly", "seven_day")].remaining_percent, 80.0
        )
        self.assertEqual(by_identity[("codex_spark", "five_hour")].remaining_percent, 90.0)
        # A missing resetsAt is valid evidence and maps to None.
        self.assertIsNone(by_identity[("codex_spark", "seven_day")].reset_at)
        self.assertEqual(
            by_identity[("codex_standard_weekly", "seven_day")].reset_at,
            datetime.fromtimestamp(_RESET_SOON, timezone.utc),
        )
        self.assertEqual(
            {pool.pool_id for pool in topology.pools},
            {f"{_RUNTIME}:base:codex_standard_weekly", f"{_RUNTIME}:base:codex_spark"},
        )
        spark_pool = next(p for p in topology.pools if p.pool_id.endswith("codex_spark"))
        # Each route governs every valid bucket window of its own bucket.
        self.assertEqual({key.window for key in spark_pool.keys}, {"five_hour", "seven_day"})
        routes = {route.route_id: route for route in topology.routes}
        self.assertEqual(set(routes), {pool.pool_id for pool in topology.pools})
        self.assertEqual(routes[f"{_RUNTIME}:base:codex_standard_weekly"].quota_lane, "Standard")
        self.assertEqual(routes[f"{_RUNTIME}:base:codex_spark"].quota_lane, "Spark")
        for route_id, route in routes.items():
            self.assertIsNone(route.account)
            self.assertEqual(route.runtime, _RUNTIME)
            self.assertEqual(route.pool_ids, (route_id,))

    def test_plus_scope_namespaces_ids_and_falls_back_to_limit_id_lane(self):
        """Ensure account scope namespaces identities and supplies a fallback lane."""
        samples, topology, account_id = normalize_rate_limits(
            _RUNTIME, "plus", _plus_response(), _OBSERVED
        )
        self.assertEqual(account_id, "acct-plus-1")
        self.assertEqual(
            {(s.lane, s.window) for s in samples},
            {("codex_plus", "five_hour"), ("codex_plus", "seven_day")},
        )
        self.assertEqual({s.target for s in samples}, {"plus"})
        (pool,) = topology.pools
        self.assertEqual(pool.pool_id, f"{_RUNTIME}:@plus:codex_plus")
        (route,) = topology.routes
        self.assertEqual(route.route_id, f"{_RUNTIME}:@plus:codex_plus")
        self.assertEqual(route.account, "plus")
        # No display limitName: quota_lane falls back to the stable limitId.
        self.assertEqual(route.quota_lane, "codex_plus")
        self.assertEqual(route.pool_ids, (pool.pool_id,))

    def test_legacy_direct_result_is_one_wrapped_bucket(self):
        """Ensure the legacy direct response shape remains supported."""
        legacy = {"rateLimits": {"primary": _window(25.0, 300)}, "accountId": "acct-legacy"}
        samples, topology, account_id = normalize_rate_limits(_RUNTIME, None, legacy, _OBSERVED)
        self.assertEqual(account_id, "acct-legacy")
        (sample,) = samples
        self.assertEqual(sample.lane, "codex")
        self.assertEqual(sample.window, "five_hour")
        self.assertEqual(sample.remaining_percent, 75.0)
        (pool,) = topology.pools
        self.assertEqual(pool.pool_id, f"{_RUNTIME}:base:codex")
        named = {"rateLimits": {"limitId": "codex_weekly", "secondary": _window(0.0, 10080)}}
        _, named_topology, _ = normalize_rate_limits(_RUNTIME, None, named, _OBSERVED)
        self.assertEqual(named_topology.pools[0].pool_id, f"{_RUNTIME}:base:codex_weekly")

    def test_wrapped_result_fixture_is_accepted(self):
        """Ensure a JSON-RPC result wrapper is normalized like its direct payload."""
        direct = normalize_rate_limits(_RUNTIME, None, _pro_response(), _OBSERVED)
        self.assertEqual(
            normalize_rate_limits(_RUNTIME, None, {"result": _pro_response()}, _OBSERVED),
            direct,
        )

    def test_by_limit_id_is_preferred_over_legacy(self):
        """Ensure the multi-bucket response takes precedence over legacy data."""
        both = _pro_response()
        both["rateLimits"] = {"primary": _window(99.0, 300)}
        samples, topology, _ = normalize_rate_limits(_RUNTIME, None, both, _OBSERVED)
        self.assertEqual(len(samples), 3)
        self.assertEqual(len(topology.pools), 2)

    def test_malformed_windows_and_buckets_are_skipped_without_dropping_valid_ones(self):
        """Ensure malformed windows are ignored while valid evidence survives."""
        response = {
            "rateLimitsByLimitId": {
                "codex_mixed": {
                    "primary": _window(50.0, 300),
                    "over": {"usedPercent": 101.0, "windowDurationMins": 300},
                    "under": {"usedPercent": -1.0, "windowDurationMins": 300},
                    "text": {"usedPercent": "50", "windowDurationMins": 300},
                    "flag": {"usedPercent": True, "windowDurationMins": 300},
                    "zero_mins": {"usedPercent": 50.0, "windowDurationMins": 0},
                    "neg_mins": {"usedPercent": 50.0, "windowDurationMins": -5},
                    "bad_reset": {"usedPercent": 50.0, "windowDurationMins": 300, "resetsAt": "soon"},
                    "neg_reset": {"usedPercent": 50.0, "windowDurationMins": 300, "resetsAt": -1},
                },
                "codex_broken": {"primary": "not-a-mapping"},
                "codex_empty": {},
                "": {"primary": _window(1.0, 300)},
            }
        }
        samples, topology, _ = normalize_rate_limits(_RUNTIME, None, response, _OBSERVED)
        (sample,) = samples
        self.assertEqual((sample.lane, sample.window), ("codex_mixed", "five_hour"))
        (pool,) = topology.pools
        self.assertEqual(pool.pool_id, f"{_RUNTIME}:base:codex_mixed")
        # Not a mapping at all is a malformed envelope, not empty evidence.
        with self.assertRaises(ValidationError):
            normalize_rate_limits(_RUNTIME, None, {"rateLimitsByLimitId": "nope"}, _OBSERVED)

    def test_window_names_cover_named_and_minute_windows(self):
        """Ensure named durations and arbitrary minute durations map stably."""
        response = {
            "rateLimitsByLimitId": {
                bucket: {"primary": _window(0.0, minutes)}
                for bucket, minutes in (("a", 300), ("b", 10080), ("c", 45), ("d", 90.5))
            }
        }
        samples, _, _ = normalize_rate_limits(_RUNTIME, None, response, _OBSERVED)
        self.assertEqual(
            {s.window for s in samples}, {"five_hour", "seven_day", "min45", "min90.5"}
        )


class CollectCodexAppserverSlicesTests(unittest.TestCase):
    """Verify per-home collection, isolation, deduplication, and freshness."""
    def _patch_reads(self, responses):
        """Patch rate-limit reads with responses keyed by queried home."""

        def fake_read(_config, home):
            """Return the fixture response associated with the queried home."""
            del _config
            result = responses[str(home)]
            if isinstance(result, BaseException):
                raise result
            return result

        return mock.patch.object(codex_rate_limits, "read_rate_limits", fake_read)

    def test_base_and_configured_account_homes_become_independent_slices(self):
        """Ensure base and configured account homes produce independent slices."""
        runtime = _runtime_config(accounts=("plus",))
        responses = {
            str(runtime.home): _pro_response(),
            str(account_runtime_home(runtime.home, "plus")): _plus_response(),
        }
        with self._patch_reads(responses):
            slices = sources.collect_codex_appserver_slices(_RUNTIME, runtime, _OBSERVED)
        self.assertEqual([s.scope_id for s in slices], ["codex:base", "codex:@plus"])
        for slice_ in slices:
            self.assertIsInstance(slice_, CapacityCollectionSlice)
            self.assertEqual(slice_.runtime, _RUNTIME)
            self.assertEqual(slice_.observed_at, _OBSERVED)
        self.assertEqual({s.samples[0].target for s in slices}, {None, "plus"})
        self.assertEqual(
            {route.account for s in slices for route in s.topology.routes}, {None, "plus"}
        )

    def test_slice_freshness_is_bounded_evidence_not_reset_distance(self):
        """Ensure provider reset distance cannot extend the 900-second shelf life."""
        # A reset days away must not keep the slice's evidence fresh for days:
        # shelf life is the bounded source interval (900s), as elsewhere.
        runtime = _runtime_config()
        response = _pro_response()
        buckets = cast(
            dict[str, dict[str, object]], response["rateLimitsByLimitId"]
        )
        for bucket in buckets.values():
            for window in bucket.values():
                if isinstance(window, dict) and "resetsAt" in window:
                    window["resetsAt"] = _RESET_DAYS_AWAY
        with self._patch_reads({str(runtime.home): response}):
            (slice_,) = sources.collect_codex_appserver_slices(_RUNTIME, runtime, _OBSERVED)
        self.assertEqual(slice_.valid_until, _OBSERVED + 900)
        self.assertEqual({s.valid_for_seconds for s in slice_.samples}, {900})

    def test_duplicate_backend_account_is_deduped_without_being_kept_or_logged(self):
        """Ensure repeated backend identities produce one redacted slice."""
        runtime = _runtime_config(accounts=("plus",))
        shared = "acct-shared-backend"
        responses = {
            str(runtime.home): _pro_response(shared),
            str(account_runtime_home(runtime.home, "plus")): _plus_response(shared),
        }
        with self._patch_reads(responses):
            slices = sources.collect_codex_appserver_slices(_RUNTIME, runtime, _OBSERVED)
        self.assertEqual([s.scope_id for s in slices], ["codex:base"])
        # The backend id is deduplication data only: never in a slice.
        self.assertNotIn(shared, repr(slices))

    def test_one_account_failure_preserves_successful_slices_and_logs_no_details(self):
        """Ensure one failed account does not affect successful accounts or leak details."""
        runtime = _runtime_config(accounts=("plus", "spark-team"))
        responses = {
            str(runtime.home): _pro_response(),
            str(account_runtime_home(runtime.home, "plus")): ConnectionError("boom token"),
            str(account_runtime_home(runtime.home, "spark-team")): _plus_response("acct-spark"),
        }
        with self.assertLogs("agent_run.capacity", level="WARNING") as captured:
            with self._patch_reads(responses):
                slices = sources.collect_codex_appserver_slices(_RUNTIME, runtime, _OBSERVED)
        self.assertEqual([s.scope_id for s in slices], ["codex:base", "codex:@spark-team"])
        joined = "\n".join(captured.output)
        self.assertIn("failed=ConnectionError", joined)
        # Neither the failure detail nor any backend account id is logged.
        self.assertNotIn("boom token", joined)
        self.assertNotIn("acct-pro-1", joined)
        self.assertNotIn("acct-spark", joined)

    def test_all_failures_yield_no_slices_so_persisted_scopes_age_naturally(self):
        """Ensure all failed homes return no slices for natural snapshot aging."""
        import tempfile

        from agent_run.state import StateStore

        runtime = _runtime_config(accounts=("plus",))
        good = {
            str(runtime.home): _pro_response(),
            str(account_runtime_home(runtime.home, "plus")): _plus_response(),
        }
        with self._patch_reads(good):
            slices = sources.collect_codex_appserver_slices(_RUNTIME, runtime, _OBSERVED)
        with tempfile.TemporaryDirectory() as directory:
            store = StateStore.initialize(Path(directory) / "state.db")
            try:
                for slice_ in slices:
                    persist_slice(store, slice_)
                rows = store.capacity_sample_history(retention=100, runtime=_RUNTIME)
                snapshots = store.capacity_route_snapshots(runtime=_RUNTIME)
                failing = {
                    str(runtime.home): {"rateLimitsByLimitId": {"broken": {"primary": "bad"}}},
                    str(account_runtime_home(runtime.home, "plus")): TimeoutError("stalled"),
                }
                with self._patch_reads(failing):
                    self.assertEqual(
                        sources.collect_codex_appserver_slices(
                            _RUNTIME, runtime, _OBSERVED + 60
                        ),
                        (),
                    )
                self.assertEqual(store.capacity_route_snapshots(runtime=_RUNTIME), snapshots)
            finally:
                store.close()
        # Multi-scope persistence: both scopes stored once, then left untouched.
        self.assertEqual(len(snapshots), 2)
        self.assertEqual({s["scope_id"] for s in snapshots}, {"codex:base", "codex:@plus"})
        self.assertEqual({row["target"] for row in rows}, {None, "plus"})


class ReadRateLimitsTests(unittest.TestCase):
    """Verify the adapter-facing read operation and cleanup behavior."""
    def test_launch_plan_is_confined_to_the_queried_home(self):
        """Ensure the rate-limit launch plan uses only the requested home."""
        runtime = _runtime_config()
        home = Path("/tmp/fictitious-home@plus")
        captured = {}

        def fake_fetch(plan, *, timeout_seconds):
            """Capture the queried launch plan and return a scripted response."""
            captured["plan"] = plan
            captured["timeout"] = timeout_seconds
            return {"accountId": "acct-x"}

        with mock.patch.object(codex_rate_limits, "fetch_rate_limits", fake_fetch):
            result = codex_rate_limits.read_rate_limits(runtime, home)
        self.assertEqual(result, {"accountId": "acct-x"})
        plan = captured["plan"]
        self.assertEqual(plan.argv, (str(runtime.binary), "app-server"))
        self.assertEqual(plan.cwd, home)
        self.assertEqual(plan.environment["CODEX_HOME"], str(home))
        self.assertEqual(plan.environment["HOME"], str(home))
        self.assertIn("PATH", plan.environment)
        self.assertEqual(captured["timeout"], 20.0)


def _plan(tmpdir, argv=("codex", "app-server"), environment=None):
    """Build a deterministic app-server launch plan for transport tests."""
    return LaunchPlan(
        argv=tuple(argv),
        cwd=Path(tmpdir),
        environment=environment if environment is not None else {},
        initial_input=None,
        runtime_stream_path=Path(tmpdir) / "runtime.jsonl",
        adapter_state={},
    )


class _FakeTransport:
    """Stands in for ``ProcessTransport`` and records its full lifecycle."""

    def __init__(self, plan, responses, state, terminate_error=None):
        """Initialize scripted responses and mutable operation state."""
        self.plan = plan
        self._responses = responses
        self._state = state
        self._terminate_error = terminate_error
        self.terminated = None
        self.closed = False

    def request(self, method, _params, *, timeout_seconds=30.0):
        """Return or raise the scripted response for an RPC method."""
        del _params
        self._state.setdefault("calls", []).append((method, timeout_seconds))
        result = self._responses.get(method, {})
        if isinstance(result, BaseException):
            raise result
        return result

    def terminate(self, grace_seconds):
        """Record termination and optionally raise its scripted failure."""
        self.terminated = grace_seconds
        if self._terminate_error is not None:
            raise self._terminate_error

    def close(self):
        """Record release of fake transport resources."""
        self.closed = True


class FetchRateLimitsTests(unittest.TestCase):
    """Verify fetching, timeout propagation, validation, and cleanup."""
    def _fetch(self, responses, *, terminate_error=None):
        """Run ``fetch_rate_limits`` on a fake transport, capturing everything."""

        import tempfile

        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        state: dict[str, object] = {}

        def fake_transport(plan):
            """Create and retain the fake transport used by the fetch."""
            transport = _FakeTransport(plan, responses, state, terminate_error)
            state["transport"] = transport
            return transport

        with mock.patch.object(app_server, "ProcessTransport", fake_transport):
            try:
                state["result"] = codex_rate_limits.fetch_rate_limits(
                    _plan(directory.name), timeout_seconds=5.0
                )
            except BaseException as error:  # surfaced via state for the assertions
                state["error"] = error
        return state

    def test_returns_direct_result_and_terminates(self):
        """Ensure successful fetches return the direct result and terminate."""
        response = _pro_response()
        state = self._fetch({"initialize": {}, "account/rateLimits/read": response})
        self.assertNotIn("error", state)
        self.assertIs(state["result"], response)
        calls = cast(list[tuple[str, float]], state["calls"])
        self.assertEqual(
            [call[0] for call in calls],
            ["initialize", "account/rateLimits/read"],
        )
        for _, timeout in calls:
            self.assertTrue(0 < timeout <= 5.0)
        transport = cast(_FakeTransport, state["transport"])
        self.assertEqual(transport.terminated, 1.0)
        self.assertFalse(transport.closed)

    def test_timeout_propagates_and_transport_is_still_terminated(self):
        """Ensure fetch timeouts propagate after transport termination."""
        state = self._fetch({"initialize": TimeoutError("deadline")})
        self.assertIsInstance(state["error"], TimeoutError)
        transport = cast(_FakeTransport, state["transport"])
        self.assertEqual(transport.terminated, 1.0)
        self.assertFalse(transport.closed)

    def test_terminate_failure_falls_back_to_close(self):
        """Ensure failed termination releases resources through close."""
        state = self._fetch(
            {"initialize": TimeoutError("deadline")},
            terminate_error=RuntimeError("terminate failed"),
        )
        self.assertIsInstance(state["error"], TimeoutError)
        transport = cast(_FakeTransport, state["transport"])
        self.assertEqual(transport.terminated, 1.0)
        # terminate failed, so the pipes are still released via close().
        self.assertTrue(transport.closed)

    def test_non_mapping_result_is_rejected_and_terminated(self):
        """Ensure non-mapping RPC results are rejected and cleaned up."""
        state = self._fetch(
            {"initialize": {}, "account/rateLimits/read": ["not", "a", "mapping"]}
        )
        self.assertIsInstance(state["error"], ValidationError)
        self.assertEqual(cast(_FakeTransport, state["transport"]).terminated, 1.0)

    def test_timeout_seconds_is_validated_before_any_spawn(self):
        """Ensure invalid timeout values are rejected before spawning."""
        spawned = []
        with mock.patch.object(
            app_server, "ProcessTransport", lambda plan: spawned.append(plan)
        ):
            for bad in (0, -1, float("inf"), True, "20"):
                with self.subTest(bad=bad):
                    with self.assertRaises(ValidationError):
                        codex_rate_limits.fetch_rate_limits(
                            _plan("."), timeout_seconds=cast(float, bad)
                        )
        self.assertEqual(spawned, [])

    def test_real_transport_times_out_and_reaps_the_child(self):
        """Ensure a real stalled child times out within the bounded test limit."""
        sleeping = _plan(
            ".",
            argv=(sys.executable, "-c", "import time; time.sleep(30)"),
            environment={"PATH": "/usr/bin:/bin"},
        )
        started = time.monotonic()
        with self.assertRaises(TimeoutError):
            codex_rate_limits.fetch_rate_limits(sleeping, timeout_seconds=1.0)
        self.assertLess(time.monotonic() - started, 20)


if __name__ == "__main__":
    unittest.main()
