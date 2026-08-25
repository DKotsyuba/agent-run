import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from agent_run.capacity.forecast import (
    RISK_HIGH,
    RISK_LOW,
    RISK_MEDIUM,
    RISK_UNKNOWN,
    build_forecasts,
)
from agent_run.capacity.history import CapacityKey, CapacitySeries, NormalizedSample


KEY = CapacityKey("codex", "requests", "5h", "gpt-5.6-sol", "app_server")


class CapacityForecastTests(unittest.TestCase):
    def test_no_samples_is_unknown_and_never_blocks(self) -> None:
        series = CapacitySeries(KEY, ())
        (forecast,) = build_forecasts([series], now=1_000_000.0)
        self.assertEqual(forecast.key, KEY)
        self.assertFalse(forecast.known)
        self.assertIsNone(forecast.remaining_percent)
        self.assertTrue(forecast.warmup)
        self.assertEqual(forecast.risk, RISK_UNKNOWN)

    def test_stale_only_sample_is_unknown(self) -> None:
        now = 1_000_000.0
        stale = NormalizedSample(
            remaining_percent=50.0, reset_at=None, observed_at=now - 3600, valid_until=now - 10
        )
        series = CapacitySeries(KEY, (stale,))
        (forecast,) = build_forecasts([series], now=now)
        self.assertFalse(forecast.known)
        self.assertEqual(forecast.risk, RISK_UNKNOWN)

    def test_single_fresh_sample_is_warmup_and_uses_remaining_thresholds(self) -> None:
        now = 1_000_000.0
        sample = NormalizedSample(
            remaining_percent=5.0, reset_at=None, observed_at=now, valid_until=None
        )
        series = CapacitySeries(KEY, (sample,))
        (forecast,) = build_forecasts([series], now=now)
        self.assertTrue(forecast.known)
        self.assertTrue(forecast.warmup)
        self.assertIsNone(forecast.burn_percent_per_hour)
        self.assertEqual(forecast.risk, RISK_HIGH)

    def test_burn_below_sustainable_pace_is_low_risk(self) -> None:
        now = 1_000_000.0
        reset_at = now + 2 * 3600
        newest = NormalizedSample(60.0, reset_at, now, None)
        oldest = NormalizedSample(80.0, reset_at, now - 3600, None)
        series = CapacitySeries(KEY, (newest, oldest))
        (forecast,) = build_forecasts([series], now=now)
        self.assertFalse(forecast.warmup)
        self.assertAlmostEqual(forecast.burn_percent_per_hour, 20.0)
        self.assertAlmostEqual(forecast.sustainable_percent_per_hour, 30.0)
        self.assertEqual(forecast.risk, RISK_LOW)

    def test_burn_above_sustainable_pace_is_medium_or_high_risk(self) -> None:
        now = 1_000_000.0
        reset_at = now + 2 * 3600
        newest_medium = NormalizedSample(60.0, reset_at, now, None)
        oldest_medium = NormalizedSample(95.0, reset_at, now - 3600, None)
        (medium,) = build_forecasts(
            [CapacitySeries(KEY, (newest_medium, oldest_medium))], now=now
        )
        self.assertEqual(medium.risk, RISK_MEDIUM)

        newest_high = NormalizedSample(60.0, reset_at, now, None)
        oldest_high = NormalizedSample(160.0, reset_at, now - 3600, None)
        (high,) = build_forecasts(
            [CapacitySeries(KEY, (newest_high, oldest_high))], now=now
        )
        self.assertEqual(high.risk, RISK_HIGH)

    def test_window_reset_starts_a_new_warmup(self) -> None:
        now = 1_000_000.0
        post_reset = NormalizedSample(90.0, now + 3600, now, None)
        pre_reset = NormalizedSample(5.0, now - 10, now - 3600, None)
        series = CapacitySeries(KEY, (post_reset, pre_reset))
        (forecast,) = build_forecasts([series], now=now)
        self.assertTrue(forecast.warmup)
        self.assertIsNone(forecast.burn_percent_per_hour)
        self.assertEqual(forecast.remaining_percent, 90.0)


if __name__ == "__main__":
    unittest.main()
