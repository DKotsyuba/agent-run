import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from agent_run.adapters.opencode.service import (
    SERVICE_HOST,
    ServiceDescriptor,
    ServiceIsolationError,
    build_service_plan,
    descriptor_path,
    read_service_descriptor,
    service_home_paths,
    verify_isolation,
    write_service_descriptor,
)
from agent_run.config import RuntimeAuthConfig, RuntimeConfig
from agent_run.errors import ValidationError


def runtime_config(binary, home, **overrides):
    values = dict(
        enabled=True,
        adapter="agent_run.adapters.opencode.adapter:ADAPTER",
        binary=Path(binary),
        home=Path(home),
        models=("MiniMaxM3",),
        service_mode="managed",
    )
    values.update(overrides)
    return RuntimeConfig(**values)


class ServiceTempCase(unittest.TestCase):
    def setUp(self):
        self._temp = tempfile.TemporaryDirectory()
        self.addCleanup(self._temp.cleanup)
        self.root = Path(self._temp.name).resolve()
        self.home = self.root / "home"
        self.home.mkdir()
        self.binary = self.root / "opencode2"
        self.binary.write_text("#!/bin/sh\n", encoding="utf-8")
        self.config = runtime_config(self.binary, self.home)

    def plan(self, **kwargs):
        return build_service_plan(self.config, self.home, port=41777, **kwargs)


class BuildServicePlanTests(ServiceTempCase):
    def test_plan_uses_generated_xdg_homes_and_disables_claude_code(self):
        plan = self.plan()
        config_home, data_home = service_home_paths(self.home)
        self.assertEqual(plan.environment["XDG_CONFIG_HOME"], str(config_home))
        self.assertEqual(plan.environment["XDG_DATA_HOME"], str(data_home))
        self.assertEqual(plan.environment["HOME"], str(self.home))
        self.assertEqual(plan.environment["OPENCODE_DISABLE_CLAUDE_CODE"], "1")
        self.assertTrue(config_home.is_relative_to(self.home))
        self.assertEqual(plan.host, SERVICE_HOST)
        self.assertEqual(plan.base_url, f"http://{SERVICE_HOST}:41777")
        self.assertIn(str(self.binary), plan.argv)

    def test_plan_creates_nothing_on_the_machine(self):
        before = sorted(path.name for path in self.home.iterdir())
        self.plan()
        self.assertEqual(sorted(path.name for path in self.home.iterdir()), before)

    def test_environment_is_a_closed_allowlist(self):
        plan = self.plan(inherited_environment={"PATH": "/usr/bin", "SECRET": "x"})
        self.assertEqual(plan.environment["PATH"], "/usr/bin")
        self.assertNotIn("SECRET", plan.environment)

    def test_declared_environment_auth_names_pass_through(self):
        config = runtime_config(
            self.binary,
            self.home,
            auth=RuntimeAuthConfig("environment", names=("OPENAI_API_KEY",)),
        )
        plan = build_service_plan(
            config, self.home, port=41777, inherited_environment={"OPENAI_API_KEY": "token"}
        )
        self.assertEqual(plan.environment["OPENAI_API_KEY"], "token")
        with self.assertRaises(ValidationError):
            build_service_plan(config, self.home, port=41777)

    def test_global_service_environment_is_refused(self):
        with self.assertRaises(ServiceIsolationError):
            self.plan(inherited_environment={"OPENCODE_SERVER": "http://127.0.0.1:4096"})

    def test_unmanaged_service_mode_is_refused(self):
        config = runtime_config(self.binary, self.home, service_mode=None)
        with self.assertRaises(ValidationError):
            build_service_plan(config, self.home, port=41777)

    def test_missing_binary_and_out_of_range_port_are_refused(self):
        config = runtime_config(self.root / "absent", self.home)
        with self.assertRaises(ValidationError):
            build_service_plan(config, self.home, port=41777)
        with self.assertRaises(ValidationError):
            self.plan_port(80)

    def plan_port(self, port):
        return build_service_plan(self.config, self.home, port=port)


class IsolationProofTests(ServiceTempCase):
    def reported(self, **overrides):
        config_home, data_home = service_home_paths(self.home)
        payload = {
            "config_home": str(config_home),
            "data_home": str(data_home),
            "host": SERVICE_HOST,
            "port": 41777,
            "pid": 4242,
            "version": "2.1.0",
        }
        payload.update(overrides)
        return payload

    def test_contained_report_yields_a_descriptor(self):
        descriptor = verify_isolation(self.plan(), self.reported())
        self.assertEqual(descriptor.port, 41777)
        self.assertEqual(descriptor.pid, 4242)
        self.assertEqual(descriptor.base_url, f"http://{SERVICE_HOST}:41777")

    def test_global_home_report_is_refused(self):
        with self.assertRaises(ServiceIsolationError):
            verify_isolation(self.plan(), self.reported(config_home="/tmp"))
        with self.assertRaises(ServiceIsolationError):
            verify_isolation(self.plan(), self.reported(data_home=str(Path.home())))

    def test_foreign_endpoint_report_is_refused(self):
        with self.assertRaises(ServiceIsolationError):
            verify_isolation(self.plan(), self.reported(port=4096))
        with self.assertRaises(ServiceIsolationError):
            verify_isolation(self.plan(), self.reported(host="0.0.0.0"))

    def test_missing_report_fields_are_refused(self):
        payload = self.reported()
        del payload["config_home"]
        with self.assertRaises(ServiceIsolationError):
            verify_isolation(self.plan(), payload)


class DescriptorFileTests(ServiceTempCase):
    def test_descriptor_roundtrip_is_private(self):
        descriptor = verify_isolation(
            self.plan(),
            {
                "config_home": str(service_home_paths(self.home)[0]),
                "data_home": str(service_home_paths(self.home)[1]),
                "host": SERVICE_HOST,
                "port": 41777,
            },
        )
        digest = write_service_descriptor(self.home, descriptor)
        self.assertEqual(len(digest), 64)
        path = descriptor_path(self.home)
        self.assertEqual(path.stat().st_mode & 0o777, 0o600)
        loaded = read_service_descriptor(self.home)
        self.assertEqual(loaded.port, 41777)
        self.assertEqual(loaded.host, SERVICE_HOST)

    def test_absent_descriptor_reads_as_none(self):
        self.assertIsNone(read_service_descriptor(self.home))

    def test_recorded_global_descriptor_is_refused(self):
        foreign = ServiceDescriptor(
            host=SERVICE_HOST, port=4096, config_home=Path("/tmp"), data_home=Path("/tmp")
        )
        write_service_descriptor(self.home, foreign)
        with self.assertRaises(ServiceIsolationError):
            read_service_descriptor(self.home)


if __name__ == "__main__":
    unittest.main()
