import io
import json
import os
import signal
import socket
import sys
import tempfile
import unittest
import urllib.error
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from agent_run.adapters.opencode.service import PASSWORD_ENV
from agent_run.adapters.home import content_hash, write_managed_file
from agent_run.adapters.opencode import service as service_module
from agent_run.adapters.opencode.http import HEALTH_PATH, OpenCodeHttpClient, RetryPolicy
from agent_run.adapters.opencode.service import (
    CONFIG_API_PATH,
    CONFIG_RELATIVE_PATH,
    GLOBAL_HEALTH_PATH,
    PINNED_VERSION,
    SERVICE_HOST,
    SERVICE_LOG_NAME,
    SERVICE_PATH,
    ServiceDescriptor,
    ServiceIsolationError,
    attach_service,
    build_service_plan,
    descriptor_path,
    read_service_descriptor,
    resolve_environment_names,
    service_home_paths,
    start_service,
    verify_config_isolation,
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
        self.binary = self.root / "opencode"
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
                "OPENCODE_DISABLE_AUTOUPDATE",
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

    def test_a_health_payload_with_no_pid_field_is_accepted(self):
        """v1 stable's real /api/health is exactly {"healthy": true}: no pid,
        no version. Its absence must not be confused with a pid of 0/None."""

        payload = self.reported()
        del payload["pid"]
        del payload["version"]
        descriptor = verify_isolation(self.plan(), payload, pid=4242, config_hash=CONFIG_HASH)
        self.assertEqual(descriptor.pid, 4242)
        self.assertIsNone(descriptor.version)


class ConfigIsolationTests(ServiceTempCase):
    """verify_config_isolation: a sentinel-key assert on the merged /config document.

    v1 has no per-source document list, only ``GET /config``'s one merged
    document, so isolation is proven by round-tripping the generated file's
    own default_agent/model pair instead of walking a list of sources.
    """

    def setUp(self):
        super().setUp()
        self.config_file = self.home / CONFIG_RELATIVE_PATH
        write_managed_file(
            self.home,
            CONFIG_RELATIVE_PATH,
            json.dumps({"default_agent": "agent-run", "model": MODEL}) + "\n",
        )

    def merged(self, **overrides):
        """The real /config shape: the one merged document, sentinel matching."""
        payload = {"default_agent": "agent-run", "model": MODEL, "share": "disabled"}
        payload.update(overrides)
        return payload

    def test_the_matching_merged_document_passes(self):
        verify_config_isolation(self.merged(), self.config_file)

    def test_a_mismatched_default_agent_is_refused_without_leaking_the_document(self):
        payload = self.merged(default_agent="global-primary")
        with self.assertRaises(ServiceIsolationError) as caught:
            verify_config_isolation(payload, self.config_file)
        message = str(caught.exception)
        self.assertIn("default_agent", message)
        self.assertIn("global-primary", message)
        self.assertNotIn("share", message)

    def test_a_mismatched_model_is_refused(self):
        payload = self.merged(model="omniroute/some-other-model")
        with self.assertRaises(ServiceIsolationError) as caught:
            verify_config_isolation(payload, self.config_file)
        self.assertIn("model", str(caught.exception))

    def test_a_non_mapping_payload_is_refused(self):
        with self.assertRaises(ServiceIsolationError) as caught:
            verify_config_isolation([{"type": "document"}], self.config_file)
        self.assertIn("did not report a JSON object", str(caught.exception))

    def test_a_generated_config_missing_a_sentinel_is_refused(self):
        write_managed_file(self.home, CONFIG_RELATIVE_PATH, '{"share": "disabled"}\n')
        with self.assertRaises(ServiceIsolationError) as caught:
            verify_config_isolation(self.merged(), self.config_file)
        self.assertIn("sentinel", str(caught.exception))


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


class FakeReply:
    """One captured HTTP reply, exactly as ``urlopen`` would hand it over."""

    def __init__(self, payload, status=200):
        self._buffer = io.BytesIO(json.dumps(payload).encode("utf-8"))
        self.status = status

    def read(self, size=-1):
        return self._buffer.read(size)

    def close(self):
        pass


class FakeOpener:
    def __init__(self, *replies):
        self.replies = list(replies)
        self.calls = []

    def __call__(self, request, timeout=None):
        self.calls.append((request.get_method(), request.full_url))
        reply = self.replies.pop(0)
        if isinstance(reply, BaseException):
            raise reply
        return reply


def http_error(code):
    return urllib.error.HTTPError("http://127.0.0.1:1/x", code, "boom", {}, None)


class FakeProcess:
    """A spawned candidate whose liveness the test controls exactly."""

    def __init__(self, pid, exit_code=None):
        self.pid = pid
        self.exit_code = exit_code

    def poll(self):
        return self.exit_code


class RecordingOps:
    """Process control that records signals instead of sending them."""

    def __init__(self, pid):
        self.pid = pid
        self.signals = []
        self.now = 0.0

    def monotonic(self):
        return self.now

    def sleep(self, seconds):
        self.now += max(0.0, seconds)

    def process_group(self, pid):
        return self.pid

    def signal_group(self, pgid, signal_number):
        self.signals.append((pgid, signal_number))
        return True

    def group_alive(self, pgid):
        return True

    def reap(self, pid):
        return None


class StartServiceTests(ServiceTempCase):
    """Starting the one managed service: proof before use, cleanup on failure."""

    def setUp(self):
        super().setUp()
        self.config_digest = write_managed_file(
            self.home,
            CONFIG_RELATIVE_PATH,
            json.dumps({"default_agent": "agent-run", "model": MODEL}) + "\n",
        )
        self.config_file = self.home / CONFIG_RELATIVE_PATH
        self.pid = os.getpid()
        self.process = FakeProcess(self.pid)
        self.ops = RecordingOps(self.pid)
        self.spawns = []
        popen = mock.patch.object(
            service_module.subprocess, "Popen", side_effect=self.record_spawn
        )
        popen.start()
        self.addCleanup(popen.stop)

    def record_spawn(self, argv, **kwargs):
        self.spawns.append((tuple(argv), kwargs))
        return self.process

    def client_factory(self, *replies):
        self.opener = FakeOpener(*replies)

        def build(base_url):
            return OpenCodeHttpClient(
                base_url,
                self.home,
                opener=self.opener,
                password="fixture-password",
                retry=RetryPolicy(attempts=1, base_seconds=0.01, cap_seconds=0.02),
                sleep=lambda seconds: None,
            )

        return build

    def start(self, *replies, **kwargs):
        kwargs.setdefault("client_factory", self.client_factory(*replies))
        kwargs.setdefault("ops", self.ops)
        kwargs.setdefault("sleep", lambda seconds: None)
        kwargs.setdefault(
            "inherited_environment", {PASSWORD_ENV: "fixture-password"}
        )
        return start_service(self.config, self.home, **kwargs)

    def healthy(self, **overrides):
        payload = {"healthy": True, "pid": self.pid, "version": "2.1.0"}
        payload.update(overrides)
        return FakeReply(payload)

    def reported_config(self, **overrides):
        """The live /config shape: the one merged document, sentinel matching."""
        payload = {"default_agent": "agent-run", "model": MODEL, "share": "disabled"}
        payload.update(overrides)
        return FakeReply(payload)

    def global_health(self, **overrides):
        """The live /global/health shape: healthy plus the pinned version."""
        payload = {"healthy": True, "version": PINNED_VERSION}
        payload.update(overrides)
        return FakeReply(payload)

    def assert_candidate_cleaned_up(self):
        self.assertEqual(
            self.ops.signals,
            [(self.pid, signal.SIGTERM), (self.pid, signal.SIGKILL)],
        )
        self.assertIsNone(read_service_descriptor(self.home))

    def test_an_unmaterialized_home_starts_nothing(self):
        self.config_file.unlink()
        with self.assertRaises(ServiceIsolationError) as caught:
            self.start()
        self.assertIn("materialize", str(caught.exception))
        self.assertEqual(self.spawns, [])

    def test_a_pre_existing_listener_is_never_adopted(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
            listener.bind((SERVICE_HOST, 0))
            listener.listen(1)
            port = int(listener.getsockname()[1])
            with self.assertRaises(ServiceIsolationError) as caught:
                self.start(port=port)
        self.assertIn("already listens", str(caught.exception))
        self.assertEqual(self.spawns, [])

    def test_one_start_spawns_one_default_serve_and_records_the_proof(self):
        started = self.start(self.healthy(), self.reported_config(), self.global_health())

        self.assertFalse(started.reused)
        self.assertEqual(len(self.spawns), 1)
        argv, spawn = self.spawns[0]
        self.assertEqual(
            argv,
            (
                str(self.binary),
                "serve",
                "--hostname",
                SERVICE_HOST,
                "--port",
                str(started.descriptor.port),
            ),
        )
        self.assertNotIn("--service", argv)
        self.assertTrue(spawn["start_new_session"])
        self.assertEqual(spawn["cwd"], str(self.home))
        self.assertEqual(spawn["env"]["PATH"], SERVICE_PATH)
        self.assertEqual(
            spawn["env"]["XDG_CONFIG_HOME"], str(service_home_paths(self.home)[0])
        )
        self.assertEqual(
            [url for _method, url in self.opener.calls],
            [
                f"{started.descriptor.base_url}{HEALTH_PATH}",
                f"{started.descriptor.base_url}{CONFIG_API_PATH}",
                f"{started.descriptor.base_url}{GLOBAL_HEALTH_PATH}",
            ],
        )
        self.assertEqual(started.descriptor.pid, self.pid)
        self.assertEqual(started.descriptor.version, "2.1.0")
        self.assertEqual(started.descriptor.config_hash, self.config_digest)
        self.assertEqual(self.ops.signals, [])

        recorded = descriptor_path(self.home)
        self.assertEqual(recorded.stat().st_mode & 0o777, 0o600)
        text = recorded.read_text(encoding="utf-8")
        self.assertNotIn("fixture-password", text)
        self.assertNotIn("password", text.lower())
        self.assertEqual(
            (self.home / SERVICE_LOG_NAME).stat().st_mode & 0o777, 0o600
        )

    def test_readiness_survives_a_refusal_before_the_service_is_up(self):
        started = self.start(
            http_error(503), self.healthy(), self.reported_config(), self.global_health()
        )
        self.assertEqual(len(self.spawns), 1)
        self.assertEqual(len(self.opener.calls), 4)
        self.assertEqual(started.descriptor.pid, self.pid)

    def test_a_v1_shaped_health_reply_without_pid_or_version_still_completes(self):
        """v1 stable's real /api/health is exactly {"healthy": true}: no pid,
        no version -- unlike the beta shape every other fixture here uses."""

        started = self.start(
            FakeReply({"healthy": True}), self.reported_config(), self.global_health()
        )
        self.assertFalse(started.reused)
        self.assertEqual(started.descriptor.pid, self.pid)
        self.assertIsNone(started.descriptor.version)

    def test_a_health_pid_mismatch_terminates_only_the_candidate(self):
        with self.assertRaises(ServiceIsolationError) as caught:
            self.start(self.healthy(pid=self.pid + 1))
        self.assertIn("does not match spawned pid", str(caught.exception))
        self.assertEqual(len(self.spawns), 1)
        self.assert_candidate_cleaned_up()

    def test_refused_credentials_fail_closed_and_clean_up(self):
        with self.assertRaises(ServiceIsolationError) as caught:
            self.start(http_error(401))
        self.assertIn("refused the managed credentials", str(caught.exception))
        self.assertEqual(len(self.spawns), 1)
        self.assert_candidate_cleaned_up()

    def test_an_early_exit_and_a_stalled_start_both_fail_closed(self):
        self.process.exit_code = 3
        with self.assertRaises(ServiceIsolationError) as exited:
            self.start()
        self.assertIn("exited with status 3", str(exited.exception))
        # Nothing is signalled: the candidate is already gone.
        self.assertEqual(self.ops.signals, [])
        self.assertIsNone(read_service_descriptor(self.home))

        self.process.exit_code = None
        clock = [0.0, 0.0, 100.0]
        with self.assertRaises(ServiceIsolationError) as stalled:
            self.start(
                http_error(503),
                http_error(503),
                monotonic=lambda: clock.pop(0),
            )
        self.assertIn("did not report healthy", str(stalled.exception))
        self.assertEqual(len(self.spawns), 2)
        self.assert_candidate_cleaned_up()

    def test_a_second_start_reuses_the_proven_service(self):
        first = self.start(self.healthy(), self.reported_config(), self.global_health())
        second = self.start()
        self.assertTrue(second.reused)
        self.assertEqual(second.descriptor.as_dict(), first.descriptor.as_dict())
        self.assertEqual(len(self.spawns), 1)
        self.assertEqual(self.opener.calls, [])

    def test_the_config_report_must_match_the_generated_sentinel(self):
        cases = (
            (
                FakeReply([{"type": "claude", "path": "/Users/pluto/.claude"}]),
                "did not report a JSON object",
            ),
            (
                self.reported_config(default_agent="global-primary"),
                "default_agent",
            ),
            (
                self.reported_config(model="omniroute/some-other-model"),
                "model",
            ),
        )
        for reply, expected in cases:
            with self.subTest(expected=expected):
                self.spawns.clear()
                self.ops.signals.clear()
                with self.assertRaises(ServiceIsolationError) as caught:
                    self.start(self.healthy(), reply)
                self.assertIn(expected, str(caught.exception))
                self.assertEqual(len(self.spawns), 1)
                self.assert_candidate_cleaned_up()

    def test_a_mismatched_pinned_version_is_refused_and_cleans_up(self):
        with self.assertRaises(ServiceIsolationError) as caught:
            self.start(
                self.healthy(),
                self.reported_config(),
                self.global_health(version="1.18.99"),
            )
        message = str(caught.exception)
        self.assertIn("1.18.99", message)
        self.assertIn(PINNED_VERSION, message)
        self.assertEqual(len(self.spawns), 1)
        self.assert_candidate_cleaned_up()


if __name__ == "__main__":
    unittest.main()
