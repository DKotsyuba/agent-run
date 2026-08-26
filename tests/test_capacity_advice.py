import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from agent_run.capacity.advice import advice_key, build_advice, capacity_label
from agent_run.capacity.forecast import RISK_HIGH, RISK_LOW, RISK_MEDIUM, RISK_UNKNOWN, CapacityForecast
from agent_run.capacity.history import CapacityKey


KEY = CapacityKey("codex", "requests", "5h", "gpt-5.6-sol", "app_server")


def _forecast(
    *,
    key: CapacityKey = KEY,
    known: bool = True,
    remaining: float | None = 40.0,
    reset_at: float | None = 1_010_000.0,
    warmup: bool = False,
    burn: float | None = 5.0,
    sustainable: float | None = 20.0,
    risk: str = RISK_LOW,
) -> CapacityForecast:
    return CapacityForecast(
        key=key,
        known=known,
        remaining_percent=remaining,
        reset_at=reset_at,
        observed_at=1_000_000.0,
        warmup=warmup,
        burn_percent_per_hour=burn,
        sustainable_percent_per_hour=sustainable,
        risk=risk,
    )


class CapacityAdviceTests(unittest.TestCase):
    def test_identity_is_preserved_from_forecast_to_advice(self) -> None:
        (advice,) = build_advice([_forecast()])
        self.assertEqual(advice.key, KEY)
        self.assertEqual(advice.risk, RISK_LOW)
        self.assertEqual(advice.recommendations, ())

    def test_recommendations_scale_with_risk(self) -> None:
        unknown, medium, high = build_advice(
            [
                _forecast(risk=RISK_UNKNOWN, remaining=None, warmup=True, burn=None, sustainable=None),
                _forecast(risk=RISK_MEDIUM),
                _forecast(risk=RISK_HIGH),
            ]
        )
        self.assertIn("unknown", unknown.recommendations[0])
        self.assertIn("pace", medium.recommendations[0])
        self.assertIn("exhaustion", high.recommendations[0])
        self.assertIn("target=gpt-5.6-sol", high.recommendations[0])
        self.assertIn("source=app_server", high.recommendations[0])
        self.assertEqual(
            capacity_label(KEY),
            "codex/requests 5h target=gpt-5.6-sol source=app_server",
        )

    def test_advice_key_ignores_observed_at_noise(self) -> None:
        stable_a = _forecast(remaining=41.0, reset_at=1_010_000.0)
        stable_b = CapacityForecast(
            key=stable_a.key,
            known=True,
            remaining_percent=42.0,  # same 5%-wide bucket as 41.0
            reset_at=1_010_010.0,  # same 300-second reset bucket
            observed_at=9_999_999.0,  # timestamp noise only
            warmup=False,
            burn_percent_per_hour=5.4,  # unbucketed field, not part of the key
            sustainable_percent_per_hour=20.1,
            risk=RISK_LOW,
        )
        key_a = advice_key(build_advice([stable_a]))
        key_b = advice_key(build_advice([stable_b]))
        self.assertEqual(key_a, key_b)

    def test_advice_key_changes_on_material_state(self) -> None:
        low = advice_key(build_advice([_forecast(risk=RISK_LOW)]))
        high = advice_key(build_advice([_forecast(risk=RISK_HIGH, remaining=5.0)]))
        self.assertNotEqual(low, high)

    def test_advice_key_changes_when_identity_differs(self) -> None:
        other_key = CapacityKey("claude", "requests", "5h", "sonnet", "cli")
        first = advice_key(build_advice([_forecast()]))
        second = advice_key(build_advice([_forecast(key=other_key)]))
        self.assertNotEqual(first, second)

    def test_advice_key_is_order_independent(self) -> None:
        other_key = CapacityKey("claude", "requests", "5h", "sonnet", "cli")
        forecasts_a = [_forecast(), _forecast(key=other_key)]
        forecasts_b = [_forecast(key=other_key), _forecast()]
        self.assertEqual(
            advice_key(build_advice(forecasts_a)), advice_key(build_advice(forecasts_b))
        )


if __name__ == "__main__":
    unittest.main()
