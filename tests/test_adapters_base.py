import inspect
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from agent_run.adapters.base import (
    ADAPTER_API_VERSION,
    Capability,
    RuntimeAdapter,
    RuntimeHealth,
    RuntimeInfo,
)
from agent_run.adapters.registry import AdapterRegistry, load_adapter
from agent_run.config import McpConfig, RuntimeConfig
from agent_run.errors import ValidationError


class FakeAdapter:
    def __init__(self, api_version=ADAPTER_API_VERSION):
        self.api_version = api_version

    def describe(self):
        return RuntimeInfo("fake", self.api_version, frozenset({Capability.STEER}))

    def validate(self, config):
        return None

    def materialize(self, config, home, *, mcp_servers, skills_root):
        return "hash"

    def probe(self, config, home):
        return RuntimeHealth(True, "1", True, None)

    def models(self, config, home):
        return ()

    def limits(self, config, home):
        return ()

    def prepare(self, request, profile, config, home, agent_dir, *, mcp_servers):
        raise AssertionError("not called while loading")

    def launch(self, plan, sink):
        raise AssertionError("not called while loading")


class LegacyAdapter(FakeAdapter):
    """Pre-T-028 shape: resolves MCP servers from ambient config instead."""

    def materialize(self, config, home):
        return "hash"

    def prepare(self, request, profile, config, home, agent_dir):
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
            (self.module(ADAPTER_API_VERSION=1.0), "API version"),
            (self.module(ADAPTER=FakeAdapter(1.0)), "reports API version"),
            (self.module(ADAPTER=object()), "missing callable"),
        ):
            with self.subTest(message=message), patch(
                "agent_run.adapters.registry.importlib.import_module",
                return_value=module,
            ), self.assertRaisesRegex(ValidationError, message):
                load_adapter("fake_module:ADAPTER")

    def test_materialize_and_prepare_require_resolved_mcp_servers(self) -> None:
        config = self.runtime()
        home = Path("/tmp/fake-home")
        servers = {"docs": McpConfig("stdio", Path("/bin/echo"))}
        arguments = {
            "materialize": (config, home),
            "prepare": (object(), object(), config, home, home / "agent"),
        }
        for method, args in arguments.items():
            with self.subTest(method=method):
                contract = inspect.signature(getattr(RuntimeAdapter, method))
                parameter = contract.parameters["mcp_servers"]
                self.assertIs(parameter.kind, inspect.Parameter.KEYWORD_ONLY)
                self.assertIs(parameter.default, inspect.Parameter.empty)
                self.assertEqual(parameter.annotation, "Mapping[str, McpConfig]")
                kwargs = {"mcp_servers": servers}
                if method == "materialize":
                    skills = contract.parameters["skills_root"]
                    self.assertIs(skills.kind, inspect.Parameter.KEYWORD_ONLY)
                    self.assertIs(skills.default, inspect.Parameter.empty)
                    kwargs["skills_root"] = home / "skills"
                with self.assertRaises(TypeError):
                    contract.bind(FakeAdapter(), *args)
                contract.bind(FakeAdapter(), *args, **kwargs)
                current = inspect.signature(getattr(FakeAdapter(), method))
                current.bind(*args, **kwargs)
                legacy = inspect.signature(getattr(LegacyAdapter(), method))
                with self.assertRaises(TypeError):
                    legacy.bind(*args, **kwargs)

    def test_registry_refuses_unknown_and_disabled_runtimes(self) -> None:
        registry = AdapterRegistry({"fake": self.runtime(enabled=False)})
        with self.assertRaisesRegex(ValidationError, "disabled"):
            registry.load("fake")
        with self.assertRaisesRegex(ValidationError, "not configured"):
            registry.load("missing")


if __name__ == "__main__":
    unittest.main()
