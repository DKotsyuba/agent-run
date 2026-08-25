import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from agent_run.adapters.base import (
    ADAPTER_API_VERSION,
    Capability,
    RuntimeHealth,
    RuntimeInfo,
)
from agent_run.adapters.registry import AdapterRegistry, load_adapter
from agent_run.config import RuntimeConfig
from agent_run.errors import ValidationError


class FakeAdapter:
    def describe(self):
        return RuntimeInfo("fake", ADAPTER_API_VERSION, frozenset({Capability.STEER}))

    def validate(self, config):
        return None

    def materialize(self, config, home):
        return "hash"

    def probe(self, config, home):
        return RuntimeHealth(True, "1", True, None)

    def models(self, config, home):
        return ()

    def limits(self, config, home):
        return ()

    def prepare(self, request, profile, config, home, agent_dir):
        raise AssertionError("not called while loading")

    def launch(self, plan, sink):
        raise AssertionError("not called while loading")


class AdapterTests(unittest.TestCase):
    def module(self, **changes):
        values = {"ADAPTER_API_VERSION": ADAPTER_API_VERSION, "ADAPTER": FakeAdapter()}
        values.update(changes)
        return SimpleNamespace(**values)

    def runtime(self, *, enabled=True):
        return RuntimeConfig(
            enabled,
            "fake_module:ADAPTER",
            Path("/bin/echo"),
            Path("/tmp/fake-home"),
            ("test",),
        )

    def test_loader_checks_api_members_and_capabilities_before_runtime_use(self) -> None:
        with patch(
            "agent_run.adapters.registry.importlib.import_module",
            return_value=self.module(),
        ):
            adapter = load_adapter("fake_module:ADAPTER", {Capability.STEER})
            self.assertEqual(adapter.describe().name, "fake")
            with self.assertRaisesRegex(ValidationError, "lacks required capabilities"):
                load_adapter("fake_module:ADAPTER", {Capability.WRITE})

        for module, message in (
            (self.module(ADAPTER_API_VERSION=2), "API version"),
            (self.module(ADAPTER=object()), "missing callable"),
        ):
            with self.subTest(message=message), patch(
                "agent_run.adapters.registry.importlib.import_module",
                return_value=module,
            ), self.assertRaisesRegex(ValidationError, message):
                load_adapter("fake_module:ADAPTER")

    def test_registry_refuses_unknown_and_disabled_runtimes(self) -> None:
        registry = AdapterRegistry({"fake": self.runtime(enabled=False)})
        with self.assertRaisesRegex(ValidationError, "disabled"):
            registry.load("fake")
        with self.assertRaisesRegex(ValidationError, "not configured"):
            registry.load("missing")


if __name__ == "__main__":
    unittest.main()
