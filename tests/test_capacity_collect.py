import sys
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from agent_run.adapters.base import (
    ADAPTER_API_VERSION,
    Capability,
    LimitSample,
    RuntimeInfo,
)
from agent_run.capacity.collect import (
    STATUS_COLLECTED,
    STATUS_FAILED,
    STATUS_UNSUPPORTED,
    collect_once,
)
from agent_run.config import Config, RuntimeConfig
from agent_run.state import StateStore


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
    def __init__(self, *, capabilities, samples=(), limits_error=None):
        self._capabilities = capabilities
        self._samples = samples
        self._limits_error = limits_error

    def describe(self) -> RuntimeInfo:
        return RuntimeInfo(
            name="fake", adapter_api_version=ADAPTER_API_VERSION, capabilities=self._capabilities
        )

    def limits(self, config, home):
        if self._limits_error is not None:
            raise self._limits_error
        return self._samples


class CapacityCollectTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.store = StateStore.initialize(Path(self.temporary.name) / "state.db")

    def tearDown(self) -> None:
        self.store.close()
        self.temporary.cleanup()

    def test_partial_failure_never_blocks_healthy_runtimes(self) -> None:
        observed = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        reset = datetime(2024, 1, 1, 18, 0, 0, tzinfo=timezone.utc)
        healthy_samples = (
            LimitSample(
                lane="requests",
                window="5h",
                remaining_percent=72.5,
                reset_at=reset,
                observed_at=observed,
                source="app_server",
                target="gpt-5.6-sol",
                valid_for_seconds=600,
            ),
            LimitSample(
                lane="tokens",
                window="5h",
                remaining_percent=None,
                reset_at=None,
                observed_at=None,
                source="app_server",
            ),
        )
        adapters = {
            "codex": FakeAdapter(
                capabilities=frozenset({Capability.LIVE_LIMITS}), samples=healthy_samples
            ),
            "claude": FakeAdapter(
                capabilities=frozenset({Capability.LIVE_LIMITS}),
                limits_error=RuntimeError("provider unreachable"),
            ),
            "opencode": FakeAdapter(capabilities=frozenset()),
        }
        config = Config(
            schema_version=1,
            runtimes={
                "codex": _runtime_config(),
                "claude": _runtime_config(),
                "opencode": _runtime_config(),
                "disabled_rt": _runtime_config(enabled=False),
            },
        )

        report = collect_once(
            self.store,
            config,
            at=1_704_110_400.0,
            loader=lambda name, runtime_config: adapters[name],
        )

        by_runtime = {result.runtime: result for result in report.results}
        self.assertEqual(set(by_runtime), {"codex", "claude", "opencode"})
        self.assertEqual(by_runtime["codex"].status, STATUS_COLLECTED)
        self.assertEqual(by_runtime["codex"].sample_count, 2)
        self.assertEqual(by_runtime["claude"].status, STATUS_FAILED)
        self.assertEqual(by_runtime["claude"].error, "RuntimeError")
        self.assertEqual(by_runtime["opencode"].status, STATUS_UNSUPPORTED)

        stored = self.store.recent_capacity_samples(at=0.0, limit=100)
        self.assertEqual(len(stored), 2)
        by_lane = {row["lane"]: row for row in stored}
        self.assertEqual(by_lane["requests"]["remaining_percent"], 72.5)
        self.assertEqual(by_lane["requests"]["observed_at"], observed.timestamp())
        self.assertEqual(by_lane["requests"]["reset_at"], reset.timestamp())
        self.assertEqual(by_lane["requests"]["valid_until"], observed.timestamp() + 600)
        self.assertIsNone(by_lane["tokens"]["remaining_percent"])
        self.assertEqual(by_lane["tokens"]["observed_at"], 1_704_110_400.0)
        self.assertIsNone(by_lane["tokens"]["valid_until"])

    def test_malformed_samples_and_raising_generators_are_runtime_local(self) -> None:
        sample = LimitSample(
            lane="requests",
            window="5h",
            remaining_percent=50.0,
            reset_at=None,
            observed_at=None,
            source="provider",
        )
        malformed = LimitSample(
            lane="requests",
            window="5h",
            remaining_percent=50.0,
            reset_at=None,
            observed_at=None,
            source="provider",
            valid_for_seconds="secret-value",  # type: ignore[arg-type]
        )

        def raising_samples():
            yield sample
            raise RuntimeError("api_key=must-not-leak")

        adapters = {
            "wrong_type": FakeAdapter(
                capabilities=frozenset({Capability.LIVE_LIMITS}), samples=(object(),)
            ),
            "malformed": FakeAdapter(
                capabilities=frozenset({Capability.LIVE_LIMITS}), samples=(malformed,)
            ),
            "raising": FakeAdapter(
                capabilities=frozenset({Capability.LIVE_LIMITS}), samples=raising_samples()
            ),
            "healthy": FakeAdapter(
                capabilities=frozenset({Capability.LIVE_LIMITS}), samples=(sample,)
            ),
        }
        config = Config(
            schema_version=1,
            runtimes={name: _runtime_config() for name in adapters},
        )
        report = collect_once(
            self.store, config, at=1.0, loader=lambda name, cfg: adapters[name]
        )
        by_runtime = {result.runtime: result for result in report.results}

        self.assertEqual(by_runtime["wrong_type"].error, "TypeError")
        self.assertEqual(by_runtime["malformed"].error, "ValueError")
        self.assertEqual(by_runtime["raising"].error, "RuntimeError")
        self.assertNotIn("api_key", by_runtime["raising"].error)
        self.assertEqual(by_runtime["healthy"].status, STATUS_COLLECTED)
        self.assertEqual(by_runtime["healthy"].sample_count, 1)

    def test_no_secrets_or_raw_payload_beyond_structured_sample_fields(self) -> None:
        sample = LimitSample(
            lane="requests",
            window="5h",
            remaining_percent=10.0,
            reset_at=None,
            observed_at=None,
            source="app_server",
        )
        adapters = {
            "codex": FakeAdapter(capabilities=frozenset({Capability.LIVE_LIMITS}), samples=(sample,))
        }
        config = Config(schema_version=1, runtimes={"codex": _runtime_config()})
        collect_once(self.store, config, at=1.0, loader=lambda name, cfg: adapters[name])
        stored = self.store.recent_capacity_samples(at=0.0, limit=10)
        payload = stored[0]["payload_json"]
        self.assertNotIn("auth", payload)
        self.assertNotIn("token", payload.lower())


if __name__ == "__main__":
    unittest.main()
