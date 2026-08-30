import json
import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from agent_run.adapters import omniroute
from agent_run.adapters.base import (
    ADAPTER_API_VERSION,
    Capability,
    LimitSample,
    RuntimeInfo,
)
from agent_run.capacity import sources
from agent_run.config import CapacityConfig, RuntimeConfig


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


class FakeAdapter:
    def __init__(self, capabilities, samples=()):
        self._capabilities = capabilities
        self._samples = samples

    def describe(self) -> RuntimeInfo:
        return RuntimeInfo(
            name="fake", adapter_api_version=ADAPTER_API_VERSION, capabilities=self._capabilities
        )

    def limits(self, config, home):
        return self._samples


class Completed:
    def __init__(self, stdout, returncode=0, stderr=""):
        self.stdout = stdout
        self.returncode = returncode
        self.stderr = stderr


_CODEXBAR_PAYLOAD = [
    {
        "provider": "codex",
        "source": "oauth",
        "usage": {
            "accountEmail": "owner@example.com",
            "dataConfidence": "exact",
            "updatedAt": "2026-08-29T12:12:53Z",
            "primary": None,
            "secondary": {
                "usedPercent": 46,
                "windowMinutes": 10080,
                "resetsAt": "2026-09-03T16:26:47Z",
                "resetDescription": "weekly",
            },
            "tertiary": {"usedPercent": 5.5, "windowMinutes": 300, "resetsAt": None},
            "extraRateWindows": [],
        },
        "pace": "ignored",
        "credits": {"balance": 0},
        "openaiDashboard": "ignored",
    }
]


class DispatchTests(unittest.TestCase):
    def test_none_source_collects_zero_samples(self) -> None:
        def load(_name, _config):
            raise AssertionError("adapter must not be loaded")

        result = sources.collect_samples(
            "codex", _runtime_config(limits_source="none"), CapacityConfig(), load
        )
        self.assertEqual(result, ())

    def test_native_source_gates_on_live_limits_capability(self) -> None:
        sample = LimitSample(
            lane="requests",
            window="5h",
            remaining_percent=50.0,
            reset_at=None,
            observed_at=None,
            source="provider",
        )
        adapters = {
            "with": FakeAdapter(frozenset({Capability.LIVE_LIMITS}), samples=(sample,)),
            "without": FakeAdapter(frozenset()),
        }

        with_live = sources.collect_samples(
            "with", _runtime_config(), CapacityConfig(), lambda name, config: adapters[name]
        )
        self.assertEqual(with_live, (sample,))
        without_live = sources.collect_samples(
            "without", _runtime_config(), CapacityConfig(), lambda name, config: adapters[name]
        )
        self.assertIsNone(without_live)

    def test_omniroute_source_uses_the_pool_without_any_adapter(self) -> None:
        sample = LimitSample(
            lane="pool",
            window="session_5h",
            remaining_percent=95.0,
            reset_at=None,
            observed_at=None,
            source="omniroute_quota_pool",
        )

        def load(_name, _config):
            raise AssertionError("adapter must not be loaded")

        with mock.patch.object(omniroute, "pool_samples", return_value=(sample,)):
            result = sources.collect_samples(
                "opencode", _runtime_config(limits_source="omniroute"), CapacityConfig(), load
            )
        self.assertEqual(result, (sample,))

    def test_codexbar_source_rejects_undocumented_providers(self) -> None:
        with self.assertLogs("agent_run.capacity", level="WARNING"):
            result = sources.collect_samples(
                "opencode", _runtime_config(limits_source="codexbar"), CapacityConfig(), None
            )
        self.assertEqual(result, ())


class CodexbarMappingTests(unittest.TestCase):
    def test_glm_maps_to_the_zai_provider(self) -> None:
        self.assertEqual(sources._CODEXBAR_PROVIDERS["glm"], "zai")

    def collect(self, payload):
        def run(argv):
            return Completed(
                json.dumps(payload)
                if not isinstance(payload, str)
                else payload
            )

        with mock.patch.object(sources, "_run_codexbar", side_effect=run):
            return sources.collect_samples(
                "codex", _runtime_config(limits_source="codexbar"), CapacityConfig(), None
            )

    def test_real_shape_maps_lanes_and_shelves_honestly(self) -> None:
        samples = self.collect(_CODEXBAR_PAYLOAD)
        self.assertEqual(len(samples), 2)
        secondary, tertiary = samples
        self.assertEqual(
            (secondary.lane, secondary.window, secondary.remaining_percent),
            ("secondary", "seven_day", 54.0),
        )
        self.assertEqual(secondary.source, "codexbar")
        self.assertIsNone(secondary.target)
        self.assertEqual(secondary.valid_for_seconds, 900)
        self.assertEqual(
            secondary.reset_at,
            datetime(2026, 9, 3, 16, 26, 47, tzinfo=timezone.utc),
        )
        self.assertEqual(
            secondary.observed_at,
            datetime(2026, 8, 29, 12, 12, 53, tzinfo=timezone.utc),
        )
        self.assertEqual(
            (tertiary.lane, tertiary.window), ("tertiary", "five_hour")
        )

    def test_first_account_only_and_ignored_sections(self) -> None:
        payload = [
            {"usage": {"updatedAt": None, "primary": {"usedPercent": 35}}},
            {"usage": {"updatedAt": None, "primary": {"usedPercent": 99}}},
        ]
        (sample,) = self.collect(payload)
        self.assertEqual(sample.remaining_percent, 65.0)
        self.assertIsNone(sample.observed_at)

    def test_unknown_window_minutes_names_itself(self) -> None:
        payload = [{"usage": {"primary": {"usedPercent": 80.0, "windowMinutes": 30}}}]
        (sample,) = self.collect(payload)
        self.assertEqual(sample.window, "min30")

    def test_bool_and_nonfinite_and_missing_usage_are_no_samples(self) -> None:
        for window in (
            {"usedPercent": True, "windowMinutes": 300},
            {"usedPercent": float("nan"), "windowMinutes": 300},
            {},
        ):
            with self.subTest(window=window):
                result = self.collect([{"usage": {"primary": window}}])
                self.assertEqual(result, ())

    def test_command_failure_and_garbage_are_no_samples(self) -> None:
        def raising_run(argv):
            raise OSError("codexbar missing")

        with mock.patch.object(sources, "_run_codexbar", side_effect=raising_run):
            self.assertEqual(
                sources.collect_samples(
                    "codex", _runtime_config(limits_source="codexbar"), CapacityConfig(), None
                ),
                (),
            )

        for stdout in ("garbage", '{"usage":{}}', "{}", "[]"):
            with self.subTest(stdout=stdout), self.assertLogs(
                "agent_run.capacity", level="WARNING"
            ):
                result = self.collect(stdout)
                self.assertEqual(result, ())

    def test_codexbar_timeout_allows_two_minutes(self) -> None:
        # 60s timed the claude provider out on every tick for hours; 120s
        # clears it while staying far under the 900s validity window.
        self.assertEqual(sources._CODEXBAR_TIMEOUT_SECONDS, 120)

    def test_bounded_error_tail_collapses_to_one_bounded_line(self) -> None:
        noisy = "first line\nsecond line " + "x" * 400
        tail = sources._bounded_error_tail(noisy)
        self.assertEqual(tail, " ".join(noisy.split())[:200])
        self.assertEqual(len(tail), 200)
        self.assertNotIn("\n", tail)
        self.assertEqual(sources._bounded_error_tail(None), "")
        self.assertEqual(sources._bounded_error_tail(b"raw bytes\n"), "raw bytes")

    def test_failure_warning_carries_the_bounded_stderr_tail(self) -> None:
        noisy = "provider exploded\n" + "x" * 400
        failed = Completed("[]", returncode=1, stderr=noisy)
        with mock.patch.object(
            sources, "_run_codexbar", return_value=failed
        ), self.assertLogs("agent_run.capacity", level="WARNING") as logs:
            result = sources.collect_samples(
                "claude", _runtime_config(limits_source="codexbar"), CapacityConfig(), None
            )
        self.assertEqual(result, ())
        self.assertEqual(len(logs.output), 1)
        message = logs.output[0]
        self.assertIn("runtime=claude", message)
        self.assertIn("rc=1", message)
        self.assertIn("stderr=provider exploded", message)
        self.assertNotIn("\n", message)


if __name__ == "__main__":
    unittest.main()
