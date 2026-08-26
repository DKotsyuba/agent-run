import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from agent_run.adapters.opencode.service import PASSWORD_ENV
from agent_run.adapters.home import content_hash, write_managed_file
from agent_run.adapters.opencode.service import (
    SERVICE_HOST,
    SERVICE_PATH,
    ServiceDescriptor,
    ServiceIsolationError,
    attach_service,
    build_service_plan,
    descriptor_path,
    read_service_descriptor,
    resolve_environment_names,
    service_home_paths,
    verify_isolation,
    write_service_descriptor,
)
from agent_run.config import RuntimeAuthConfig, RuntimeConfig
from agent_run.errors import ValidationError


MODEL = "omniroute/deepseek-v4-pro"
CONFIG_HASH = "a" * 64
CONFIG_NAME = "generated.json"


def runtime_config(binary, home, **overrides):
    values = dict(
        enabled=True,
        adapter="agent_run.adapters.opencode.adapter:ADAPTER",
        binary=Path(binary),
        home=Path(home),
        models=(MODEL,),
        service_mode="managed",
    )
    values.update(overrides)
    return RuntimeConfig(**values)


class ServiceTempCase(unittest.TestCase):
    def setUp(self):
        self._auth = mock.patch.dict(os.environ, {PASSWORD_ENV: "fixture-password"})
        self._auth.start()
        self.addCleanup(self._auth.stop)
        self._temp = tempfile.TemporaryDirectory()
        self.addCleanup(self._temp.cleanup)
        self.root = Path(self._temp.name).resolve()
        self.home = self.root / "home"
        self.home.mkdir()
        self.binary = self.root / "opencode2"
        self.binary.write_text("#!/bin/sh\n", encoding="utf-8")
        self.config = runtime_config(self.binary, self.home)

    def plan(self, **kwargs):
        kwargs.setdefault("inherited_environment", {PASSWORD_ENV: "fixture-password"})
        return build_service_plan(self.config, self.home, port=41777, **kwargs)


class BuildServicePlanTests(ServiceTempCase):
    def test_plan_uses_generated_xdg_homes_and_disables_claude_code(self):
        plan = self.plan()
        config_home, data_home = service_home_paths(self.home)
        self.assertEqual(plan.environment["XDG_CONFIG_HOME"], str(config_home))
        self.assertEqual(plan.environment["XDG_DATA_HOME"], str(data_home))
        self.assertEqual(plan.environment["HOME"], str(self.home))
        self.assertEqual(plan.environment["OPENCODE_DISABLE_CLAUDE_CODE"], "1")
        self.assertEqual(plan.environment[PASSWORD_ENV], "fixture-password")
        self.assertTrue(config_home.is_relative_to(self.home))
        self.assertEqual(plan.host, SERVICE_HOST)
        self.assertEqual(plan.base_url, f"http://{SERVICE_HOST}:41777")
        self.assertIn(str(self.binary), plan.argv)

    def test_plan_creates_nothing_on_the_machine(self):
        before = sorted(path.name for path in self.home.iterdir())
        self.plan()
        self.assertEqual(sorted(path.name for path in self.home.iterdir()), before)

    def test_path_is_deterministic_and_never_inherited(self):
        plan = self.plan(inherited_environment={"PATH": "/tmp/shim:/usr/bin"})
        self.assertEqual(plan.environment["PATH"], SERVICE_PATH)

    def test_ambient_secrets_never_reach_the_child(self):
        plan = self.plan(
            inherited_environment={
                PASSWORD_ENV: "fixture-password",
                "AWS_SECRET_ACCESS_KEY": "leak",
                "ANTHROPIC_API_KEY": "leak",
                "SECRET": "leak",
            }
        )
        self.assertEqual(
            sorted(plan.environment),
            [
                "HOME",
                "OPENCODE_DISABLE_CLAUDE_CODE",
                PASSWORD_ENV,
                "PATH",
                "XDG_CACHE_HOME",
                "XDG_CONFIG_HOME",
                "XDG_DATA_HOME",
                "XDG_STATE_HOME",
            ],
        )

    def test_declared_environment_auth_names_pass_through(self):
        config = runtime_config(
            self.binary,
            self.home,
            auth=RuntimeAuthConfig("environment", names=("OPENAI_API_KEY",)),
        )
        inherited = {PASSWORD_ENV: "fixture-password", "OPENAI_API_KEY": "token"}
        plan = build_service_plan(
            config, self.home, port=41777, inherited_environment=inherited
        )
        self.assertEqual(plan.environment["OPENAI_API_KEY"], "token")
        with self.assertRaises(ValidationError):
            build_service_plan(config, self.home, port=41777)
        with self.assertRaises(ValidationError):
            build_service_plan(
                config,
                self.home,
                port=41777,
                inherited_environment={
                    PASSWORD_ENV: "fixture-password",
                    "OPENAI_API_KEY": "  ",
                },
            )

    def test_global_service_environment_is_refused(self):
        with self.assertRaises(ServiceIsolationError):
            self.plan(inherited_environment={"OPENCODE_SERVER": "http://127.0.0.1:4096"})

    def test_auth_may_not_forward_an_attach_variable(self):
        config = runtime_config(
            self.binary,
            self.home,
            auth=RuntimeAuthConfig("environment", names=("OPENCODE_API_KEY",)),
        )
        with self.assertRaises(ServiceIsolationError):
            build_service_plan(
                config,
                self.home,
                port=41777,
                inherited_environment={"OPENCODE_API_KEY": "token"},
            )

    def test_a_proven_service_yields_no_second_serve(self):
        plan = self.plan(argv=())
        self.assertEqual(plan.argv, ())
        self.assertEqual(plan.environment["XDG_CONFIG_HOME"], str(service_home_paths(self.home)[0]))

    def test_unmanaged_service_mode_is_refused(self):
        config = runtime_config(self.binary, self.home, service_mode=None)
        with self.assertRaises(ValidationError):
            build_service_plan(config, self.home, port=41777)

    def test_missing_binary_and_out_of_range_port_are_refused(self):
        config = runtime_config(self.root / "absent", self.home)
        with self.assertRaises(ValidationError):
            build_service_plan(config, self.home, port=41777)
        with self.assertRaises(ValidationError):
            build_service_plan(self.config, self.home, port=80)


class ResolveEnvironmentTests(unittest.TestCase):
    def test_declared_names_are_read_from_the_ambient_environment(self):
        self.assertEqual(
            resolve_environment_names(("TOKEN",), {"TOKEN": "v", "OTHER": "x"}, what="mcp"),
            {"TOKEN": "v"},
        )

    def test_unset_blank_and_literal_values_are_refused(self):
        with self.assertRaises(ValidationError):
            resolve_environment_names(("TOKEN",), {}, what="mcp")
        with self.assertRaises(ValidationError):
            resolve_environment_names(("TOKEN",), {"TOKEN": "   "}, what="mcp")
        with self.assertRaises(ValidationError):
            resolve_environment_names(("sk-live-secret",), {}, what="mcp")

    def test_attach_variables_are_refused_as_names(self):
        with self.assertRaises(ServiceIsolationError):
            resolve_environment_names(
                ("OPENCODE_SERVER",), {"OPENCODE_SERVER": "http://x"}, what="mcp"
            )


class IsolationProofTests(ServiceTempCase):
    def reported(self, **overrides):
        payload = {
            "healthy": True,
            "pid": 4242,
            "version": "2.1.0",
        }
        payload.update(overrides)
        return payload

    def prove(self, **overrides):
        return verify_isolation(self.plan(), self.reported(**overrides), pid=4242, config_hash=CONFIG_HASH)

    def test_contained_report_yields_a_descriptor(self):
        descriptor = self.prove()
        self.assertEqual(descriptor.port, 41777)
        self.assertEqual(descriptor.pid, 4242)
        self.assertEqual(descriptor.config_hash, CONFIG_HASH)
        self.assertEqual(descriptor.base_url, f"http://{SERVICE_HOST}:41777")

    def test_health_only_proof_does_not_trust_global_or_endpoint_fields(self):
        descriptor = self.prove(config_home="/tmp", data_home=str(Path.home()), port=4096, host="0.0.0.0")
        self.assertEqual(descriptor.base_url, f"http://{SERVICE_HOST}:41777")

    def test_missing_pid_or_config_hash_is_refused(self):
        with self.assertRaises(ServiceIsolationError):
            self.prove(pid=None)
        with self.assertRaises(ServiceIsolationError):
            self.prove(pid=0)
        with self.assertRaises(ServiceIsolationError):
            verify_isolation(self.plan(), self.reported(), pid=4242, config_hash="not-a-hash")

    def test_missing_report_fields_are_refused(self):
        payload = self.reported()
        del payload["healthy"]
        with self.assertRaises(ServiceIsolationError):
            verify_isolation(self.plan(), payload, pid=4242, config_hash=CONFIG_HASH)


class DescriptorFileTests(ServiceTempCase):
    def descriptor(self, **overrides):
        config_home, data_home = service_home_paths(self.home)
        values = dict(
            host=SERVICE_HOST,
            port=41777,
            config_home=config_home,
            data_home=data_home,
            pid=os.getpid(),
            config_hash=self.config_digest,
            version="2.1.0",
        )
        values.update(overrides)
        return ServiceDescriptor(**values)

    def setUp(self):
        super().setUp()
        self.config_digest = write_managed_file(self.home, CONFIG_NAME, '{"generated": true}\n')
        self.config_file = self.home / CONFIG_NAME

    def test_descriptor_roundtrip_is_private(self):
        digest = write_service_descriptor(self.home, self.descriptor())
        self.assertEqual(len(digest), 64)
        path = descriptor_path(self.home)
        self.assertEqual(path.stat().st_mode & 0o777, 0o600)
        loaded = read_service_descriptor(self.home)
        self.assertEqual((loaded.port, loaded.host), (41777, SERVICE_HOST))
        self.assertEqual(loaded.config_hash, self.config_digest)

    def test_absent_descriptor_reads_as_none_and_never_attaches(self):
        self.assertIsNone(read_service_descriptor(self.home))
        with self.assertRaises(ServiceIsolationError) as caught:
            attach_service(self.home, self.config_file)
        self.assertIn("unproven", str(caught.exception))

    def test_recorded_global_descriptor_is_refused(self):
        write_service_descriptor(
            self.home, self.descriptor(port=4096, config_home=Path("/tmp"), data_home=Path("/tmp"))
        )
        with self.assertRaises(ServiceIsolationError):
            read_service_descriptor(self.home)

    def test_attach_reproves_pid_endpoint_isolation_and_config_hash(self):
        write_service_descriptor(self.home, self.descriptor())
        attached = attach_service(self.home, self.config_file)
        self.assertEqual(attached.pid, os.getpid())

        with self.assertRaises(ServiceIsolationError) as gone:
            attach_service(self.home, self.config_file, is_alive=lambda pid: False)
        self.assertIn("is gone", str(gone.exception))

        write_managed_file(self.home, CONFIG_NAME, '{"generated": false}\n')
        with self.assertRaises(ServiceIsolationError) as changed:
            attach_service(self.home, self.config_file)
        self.assertIn("changed after the service was proven", str(changed.exception))
        self.assertNotEqual(content_hash('{"generated": false}\n'), self.config_digest)

    def test_attach_refuses_a_missing_generated_config(self):
        write_service_descriptor(self.home, self.descriptor())
        with self.assertRaises(ServiceIsolationError):
            attach_service(self.home, self.home / "absent.json")

    def test_attach_refuses_missing_or_blank_password(self):
        write_service_descriptor(self.home, self.descriptor())
        for environment in ({}, {PASSWORD_ENV: "   "}):
            with self.subTest(environment=environment):
                with mock.patch.dict(os.environ, environment, clear=True):
                    with self.assertRaises(ServiceIsolationError) as caught:
                        attach_service(self.home, self.config_file)
                self.assertEqual(
                    str(caught.exception),
                    f"{PASSWORD_ENV} must be set to a nonblank value",
                )


if __name__ == "__main__":
    unittest.main()
