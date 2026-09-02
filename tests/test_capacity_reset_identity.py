import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from agent_run.capacity.forecast import (
    RISK_HIGH,
    RISK_LOW,
    RISK_UNKNOWN,
    build_forecasts,
)
from agent_run.capacity.history import CapacityKey, CapacitySeries, NormalizedSample

KEY = CapacityKey("codex", "requests", "5h", "gpt-5.6-sol", "app_server")

# Reset instants one live Claude weekly cycle reported for the same shared
# reset: every capture in the run carried a slightly different float epoch
# second around 1788469200, and the observed spread (0.97052s) stays inside
# one second while crossing the integer-second boundary in both directions.
JITTERED_RESETS = (
    1788469200.198643,
    1788469200.164472,
    1788469200.50504,
    1788469199.53452,
)


def sample(
    remaining_percent: float, reset_at: float | None, observed_at: float | None
) -> NormalizedSample:
    """Build a normalized sample with unbounded freshness for one reset."""

    return NormalizedSample(
        remaining_percent=remaining_percent,
        reset_at=reset_at,
        observed_at=observed_at,
        valid_until=None,
    )


class CapacityResetIdentityTests(unittest.TestCase):
    """Samples of one shared reset instant must supply burn evidence together."""

    def test_jittered_resets_across_an_hour_supply_burn_evidence(self) -> None:
        # Four captures of one weekly cycle, each with its own jittered
        # reset_at straddling an integer second, span a full hour. Before the
        # tolerance these grouped as four single-sample windows and the lane
        # stayed in warmup forever.
        now = JITTERED_RESETS[0] - 7200
        observations = (now, now - 1200, now - 2400, now - 3600)
        remainings = (40.0, 55.0, 57.5, 60.0)
        series = CapacitySeries(
            KEY,
            tuple(
                sample(remaining, reset, observed)
                for remaining, reset, observed in zip(
                    remainings, JITTERED_RESETS, observations
                )
            ),
        )
        (forecast,) = build_forecasts([series], now=now)
        self.assertTrue(forecast.known)
        self.assertFalse(forecast.warmup)
        self.assertEqual(forecast.reset_at, JITTERED_RESETS[0])
        self.assertEqual(forecast.burn_percent_per_hour, 20.0)
        self.assertEqual(forecast.burn_span_seconds, 3600.0)
        self.assertEqual(
            forecast.sustainable_percent_per_hour,
            40.0 / ((JITTERED_RESETS[0] - now) / 3600),
        )
        self.assertEqual(forecast.risk, RISK_LOW)

    def test_jitter_under_tolerance_escalates_once_the_span_gate_opens(self) -> None:
        # The same jittered grouping must still be allowed to escalate risk:
        # two jittered captures two hours apart give the evidence the span
        # gate needs.
        # The lane sits 48 hours from its reset, so the sustainable pace is
        # 88/48 per hour and the 6/hour burn clears the high multiplier.
        now = JITTERED_RESETS[0] - 48 * 3600
        series = CapacitySeries(
            KEY,
            (
                sample(88.0, JITTERED_RESETS[0], now),
                sample(100.0, JITTERED_RESETS[2], now - 2 * 3600),
            ),
        )
        (forecast,) = build_forecasts([series], now=now)
        self.assertFalse(forecast.warmup)
        self.assertEqual(forecast.burn_percent_per_hour, 6.0)
        self.assertEqual(forecast.burn_span_seconds, 7200.0)
        self.assertEqual(forecast.risk, RISK_HIGH)

    def test_young_jittered_history_stays_below_the_span_gate(self) -> None:
        # Ten minutes of jittered history reports a burn rate but is still too
        # thin to escalate on.
        now = JITTERED_RESETS[0] - 7200
        series = CapacitySeries(
            KEY,
            (
                sample(99.0, JITTERED_RESETS[0], now),
                sample(100.0, JITTERED_RESETS[1], now - 600),
            ),
        )
        (forecast,) = build_forecasts([series], now=now)
        self.assertFalse(forecast.warmup)
        self.assertEqual(forecast.burn_percent_per_hour, 6.0)
        self.assertEqual(forecast.burn_span_seconds, 600.0)
        self.assertEqual(forecast.risk, RISK_LOW)

    def test_reset_already_passed_at_latest_observation_stays_separate(self) -> None:
        # A real rollover: the older reset timestamp is under a second away
        # from the newest one, but it had already passed when the newest
        # capture was taken, so the windows are genuinely different cycles.
        # The only usable "now" sits between the two resets: the older reset
        # (1788469199.53452) has already passed, the newest (1788469200.50504)
        # has not, and the newest capture is what makes the lane fresh.
        now = 1788469200.2
        series = CapacitySeries(
            KEY,
            (
                sample(90.0, JITTERED_RESETS[2], now),
                sample(5.0, JITTERED_RESETS[3], now - 3600),
            ),
        )
        (forecast,) = build_forecasts([series], now=now)
        self.assertTrue(forecast.known)
        self.assertTrue(forecast.warmup)
        self.assertIsNone(forecast.burn_percent_per_hour)
        self.assertIsNone(forecast.burn_span_seconds)
        self.assertEqual(forecast.reset_at, JITTERED_RESETS[2])
        self.assertEqual(forecast.remaining_percent, 90.0)

    def test_resets_over_tolerance_stay_separate(self) -> None:
        now = JITTERED_RESETS[0]
        series = CapacitySeries(
            KEY,
            (
                sample(90.0, JITTERED_RESETS[0], now),
                sample(5.0, 1788469198.5, now - 3600),
            ),
        )
        (forecast,) = build_forecasts([series], now=now)
        self.assertTrue(forecast.warmup)
        self.assertIsNone(forecast.burn_percent_per_hour)
        self.assertIsNone(forecast.burn_span_seconds)

    def test_exact_equal_resets_still_group(self) -> None:
        now = JITTERED_RESETS[0] - 7200
        reset_at = JITTERED_RESETS[0]
        series = CapacitySeries(
            KEY, (sample(40.0, reset_at, now), sample(60.0, reset_at, now - 3600))
        )
        (forecast,) = build_forecasts([series], now=now)
        self.assertFalse(forecast.warmup)
        self.assertEqual(forecast.burn_percent_per_hour, 20.0)
        self.assertEqual(forecast.burn_span_seconds, 3600.0)
        self.assertEqual(forecast.reset_at, reset_at)

    def test_none_reset_matches_only_none_reset(self) -> None:
        now = 1_000_000.0
        unnamed = CapacitySeries(
            KEY, (sample(40.0, None, now), sample(60.0, None, now - 3600))
        )
        (unnamed_forecast,) = build_forecasts([unnamed], now=now)
        self.assertFalse(unnamed_forecast.warmup)
        self.assertEqual(unnamed_forecast.burn_percent_per_hour, 20.0)

        mixed = CapacitySeries(
            KEY,
            (sample(40.0, None, now), sample(60.0, JITTERED_RESETS[0], now - 3600)),
        )
        (mixed_forecast,) = build_forecasts([mixed], now=now)
        self.assertTrue(mixed_forecast.warmup)
        self.assertIsNone(mixed_forecast.burn_percent_per_hour)
        self.assertIsNone(mixed_forecast.reset_at)

    def test_missing_latest_observed_at_keeps_exact_only_grouping(self) -> None:
        # Without a latest observation time there is no way to tell a jittered
        # copy from a rolled-over cycle, so only exact equality may group.
        now = JITTERED_RESETS[0] - 7200
        series = CapacitySeries(
            KEY,
            (
                NormalizedSample(40.0, JITTERED_RESETS[0], None, None),
                sample(60.0, JITTERED_RESETS[1], now - 3600),
            ),
        )
        (forecast,) = build_forecasts([series], now=now)
        self.assertTrue(forecast.known)
        self.assertTrue(forecast.warmup)
        self.assertIsNone(forecast.burn_percent_per_hour)

    def test_stale_and_future_latest_stay_unknown_despite_jitter(self) -> None:
        now = JITTERED_RESETS[0] - 7200
        stale = CapacitySeries(
            KEY,
            (
                NormalizedSample(
                    remaining_percent=40.0,
                    reset_at=JITTERED_RESETS[0],
                    observed_at=now,
                    valid_until=now - 1,
                ),
                sample(60.0, JITTERED_RESETS[1], now - 3600),
            ),
        )
        (stale_forecast,) = build_forecasts([stale], now=now)
        self.assertFalse(stale_forecast.known)
        self.assertEqual(stale_forecast.risk, RISK_UNKNOWN)

        future = CapacitySeries(
            KEY,
            (
                sample(40.0, JITTERED_RESETS[0], now + 60),
                sample(60.0, JITTERED_RESETS[1], now - 3600),
            ),
        )
        (future_forecast,) = build_forecasts([future], now=now)
        self.assertFalse(future_forecast.known)

    def test_past_reset_at_latest_stays_unknown(self) -> None:
        now = JITTERED_RESETS[0]
        series = CapacitySeries(
            KEY, (sample(40.0, now - 0.5, now), sample(60.0, now - 0.9, now - 3600))
        )
        (forecast,) = build_forecasts([series], now=now)
        self.assertFalse(forecast.known)


if __name__ == "__main__":
    unittest.main()
