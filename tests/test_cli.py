import contextlib
import io
import json
import os
import shutil
import sys
import tarfile
import tempfile
import tomllib
import unittest
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType, SimpleNamespace
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from agent_run import cli
from agent_run.domain import AgentId, AgentStatus, StartRequest
from agent_run.errors import ValidationError
from agent_run.service import MessageView, TranscriptPage


AGENT_ID = "ag-20260826-120000-0123456789"


@dataclass(frozen=True)
class FakeStart:
    agent_id: str = AGENT_ID
    created: bool = True


@dataclass(frozen=True)
class FakeView:
    status: AgentStatus
    path: Path
    metadata: MappingProxyType


class FakeService:
    def __init__(self):
        self.calls = []
        self.request = None
        self.error = None

    def _return(self, name, value):
        self.calls.append(name)
        if self.error is not None:
            raise self.error
        return value

    def start(self, request):
        self.request = request
        return self._return("start", FakeStart())

    def bind(self, agent_id, ref):
        return self._return("bind", {"agent_id": agent_id, "orchestrator": ref})

    def cancel(self, agent_id):
        return self._return("cancel", {"agent_id": agent_id, "status": "cancelling"})

    def steer(self, agent_id, text):
        return self._return("steer", {"agent_id": agent_id, "text": text})

    def get(self, agent_id):
        return self._return(
            "get",
            FakeView(
                AgentStatus.RUNNING,
                Path("/tmp/answer.md"),
                MappingProxyType({"agent_id": agent_id}),
            ),
        )

    def list(self, query):
        return self._return("list", query)

    def summary(self, **kwargs):
        return self._return("summary", kwargs)

    def transcript(self, agent_id, cursor=0, limit=200):
        self.calls.append(("transcript", cursor, limit))
        if cursor == 0:
            return TranscriptPage(
                AgentId(AGENT_ID),
                (MessageView(1, 1.0, "assistant", None, "one", None),),
                0,
                limit,
                1,
                False,
            )
        return TranscriptPage(
            AgentId(AGENT_ID),
            (MessageView(2, 2.0, "assistant", None, "two", None),),
            cursor,
            limit,
            None,
            True,
        )

    def answer(self, agent_id):
        return self._return("answer", {"agent_id": agent_id, "available": False})

    def models(self):
        return self._return("models", MappingProxyType({"codex": ("model",)}))

    def limits(self):
        return self._return("limits", {"risk": AgentStatus.RUNNING})

    def context(self, ref):
        return self._return("context", {"orchestrator": ref, "injected": True})

    def capacity_collect(self):
        return self._return("capacity_collect", {"collected": True})

    def delivery_status(self, agent_id):
        return self._return("delivery_status", {"agent_id": agent_id})

    def delivery_cancel(self, delivery_id):
        return self._return("delivery_cancel", {"delivery_id": delivery_id})

    def delivery_dispatch(self):
        return self._return("delivery_dispatch", {"delivered": 1})

    def hook_context(self, payload, transport="codex_queue"):
        return self._return(
            "hook_context",
            {
                "hookSpecificOutput": {
                    "hookEventName": "UserPromptSubmit",
                    "additionalContext": "context",
                }
            },
        )

    def hook_bind(self, payload, transport="codex_queue"):
        return self._return("hook_bind", payload)

    def init(self):
        return self._return("init", {"initialized": True})

    def doctor(self):
        return self._return("doctor", {"healthy": True})


class CliTests(unittest.TestCase):
    def run_cli(self, argv, *, service=None, stdin=""):
        stdout = io.StringIO()
        stderr = io.StringIO()
        code = cli.main(
            argv,
            service=service or FakeService(),
            stdin=io.StringIO(stdin),
            stdout=stdout,
            stderr=stderr,
        )
        return code, stdout.getvalue(), stderr.getvalue()

    def test_start_decodes_the_full_request_and_returns_immediately(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            workdir = root / "work"
            read_root = root / "read"
            workdir.mkdir()
            read_root.mkdir()
            service = FakeService()
            code, output, error = self.run_cli(
                [
                    "start",
                    "--runtime",
                    "codex",
                    "--model",
                    "model",
                    "--profile",
                    "review",
                    "--task",
                    "-",
                    "--workdir",
                    str(workdir),
                    "--write",
                    "--effort",
                    "high",
                    "--timeout",
                    "42",
                    "--read-root",
                    str(read_root),
                    "--output-schema",
                    '{"type":"object"}',
                    "--request-id",
                    "request-1",
                    "--session-transport",
                    "codex_queue",
                    "--session-id",
                    "session-1",
                    "--session-turn-id",
                    "turn-1",
                ],
                service=service,
                stdin="do work",
            )
        self.assertEqual(code, 0)
        self.assertEqual(error, "")
        self.assertEqual(json.loads(output), {"agent_id": AGENT_ID, "created": True})
        request = service.request
        self.assertIsInstance(request, StartRequest)
        self.assertEqual(request.task, "do work")
        self.assertTrue(request.write)
        self.assertFalse(request.fast)
        self.assertEqual(request.effort, "high")
        self.assertEqual(request.timeout_seconds, 42)
        self.assertEqual(request.read_roots, (read_root.resolve(),))
        self.assertEqual(request.output_schema, {"type": "object"})
        self.assertEqual(request.request_id, "request-1")
        self.assertEqual(request.orchestrator.external_turn_id, "turn-1")

    def test_start_preserves_omitted_and_explicit_timeout(self):
        with tempfile.TemporaryDirectory() as directory:
            base = [
                "start", "--runtime", "fake", "--model", "model",
                "--profile", "p", "--task", "t", "--workdir", directory,
            ]
            omitted = FakeService()
            explicit = FakeService()
            self.assertEqual(self.run_cli(base, service=omitted)[0], 0)
            self.assertEqual(
                self.run_cli(base + ["--timeout", "480"], service=explicit)[0], 0
            )
        self.assertIsNone(omitted.request.timeout_seconds)
        self.assertEqual(explicit.request.timeout_seconds, 480)

    def test_start_fast_flag_reaches_the_request(self):
        with tempfile.TemporaryDirectory() as directory:
            service = FakeService()
            self.assertEqual(self.run_cli(["start", "--runtime", "codex", "--model", "model", "--profile", "p", "--task", "t", "--workdir", directory, "--fast"], service=service)[0], 0)
        self.assertTrue(service.request.fast)

    def test_json_supports_dataclasses_enums_paths_and_mappingproxy(self):
        code, output, error = self.run_cli(["status", AGENT_ID])
        self.assertEqual(code, 0)
        self.assertEqual(error, "")
        self.assertEqual(
            json.loads(output),
            {
                "metadata": {"agent_id": AGENT_ID},
                "path": "/tmp/answer.md",
                "status": "running",
            },
        )

    def test_transcript_is_bounded_unless_full_is_explicit(self):
        ordinary = FakeService()
        code, output, _error = self.run_cli(
            ["transcript", AGENT_ID, "--limit", "1"], service=ordinary
        )
        self.assertEqual(code, 0)
        self.assertFalse(json.loads(output)["complete"])
        self.assertEqual(ordinary.calls, [("transcript", 0, 1)])

        full = FakeService()
        code, output, _error = self.run_cli(
            ["transcript", AGENT_ID, "--limit", "1", "--full"], service=full
        )
        payload = json.loads(output)
        self.assertEqual(code, 0)
        self.assertEqual(payload["pages"], 2)
        self.assertEqual([message["content"] for message in payload["messages"]], ["one", "two"])
        self.assertEqual(full.calls, [("transcript", 0, 1), ("transcript", 1, 1)])

    def test_transcript_follow_polls_without_duplicates_until_terminal_and_drained(self):
        class FollowService(FakeService):
            def __init__(self):
                super().__init__()
                self.statuses = [AgentStatus.RUNNING, AgentStatus.SUCCEEDED]

            def get(self, agent_id):
                self.calls.append("get")
                return FakeView(
                    self.statuses.pop(0), Path("/tmp/answer.md"), MappingProxyType({})
                )

            def transcript(self, agent_id, cursor=0, limit=200):
                self.calls.append(("transcript", cursor, limit))
                seq = cursor + 1
                return TranscriptPage(
                    AgentId(AGENT_ID),
                    (MessageView(seq, float(seq), "assistant", None, str(seq), None),),
                    cursor,
                    limit,
                    None,
                    True,
                )

        service = FollowService()
        with patch("time.sleep") as sleep:
            code, output, error = self.run_cli(
                ["transcript", AGENT_ID, "--limit", "1", "--follow"],
                service=service,
            )
        payload = json.loads(output)
        self.assertEqual((code, error), (0, ""))
        self.assertEqual([item["seq"] for item in payload["messages"]], [1, 2])
        self.assertEqual(
            service.calls,
            [("transcript", 0, 1), "get", ("transcript", 1, 1), "get"],
        )
        sleep.assert_called_once()

        code, output, error = self.run_cli(
            ["transcript", AGENT_ID, "--follow", "--full"], service=FakeService()
        )
        self.assertEqual((code, output), (2, ""))
        self.assertIn("not allowed with argument", error)

    def test_expected_errors_are_stable_json_but_unexpected_faults_propagate(self):
        expected = FakeService()
        expected.error = ValidationError("bad request")
        code, output, error = self.run_cli(["status", AGENT_ID], service=expected)
        self.assertEqual(code, 2)
        self.assertEqual(output, "")
        self.assertEqual(json.loads(error)["error"]["type"], "ValidationError")

        code, output, error = self.run_cli(["start"])
        self.assertEqual(code, 2)
        self.assertEqual(output, "")
        self.assertEqual(json.loads(error)["error"]["type"], "ValidationError")

        unexpected = FakeService()
        unexpected.error = RuntimeError("bug")
        with self.assertRaisesRegex(RuntimeError, "bug"):
            self.run_cli(["status", AGENT_ID], service=unexpected)

    def test_bootstrap_failure_error_envelope_carries_the_agent_id(self):
        bootstrapped = FakeService()
        failure = ValidationError(
            "detached supervisor died before session proof at stage 'import': "
            "ModuleNotFoundError: no module named agent_run.adapters"
        )
        failure.agent_id = AGENT_ID
        failure.failure_kind = "supervisor_start_failed"
        failure.failure_stage = "import"
        failure.failure_text = "ModuleNotFoundError: no module named agent_run.adapters"
        bootstrapped.error = failure

        code, output, error = self.run_cli(["status", AGENT_ID], service=bootstrapped)
        self.assertEqual(code, 2)
        self.assertEqual(output, "")
        payload = json.loads(error)["error"]
        self.assertEqual(payload["agent_id"], AGENT_ID)
        self.assertEqual(payload["status"], "failed")
        self.assertEqual(payload["failure_kind"], "supervisor_start_failed")
        self.assertEqual(payload["failure_stage"], "import")

    def test_producer_shims_and_all_top_level_commands_parse(self):
        service = FakeService()
        cases = (
            (["bind", AGENT_ID, "--session-transport", "codex_queue", "--session-id", "s"], "bind"),
            (["cancel", AGENT_ID], "cancel"),
            (["steer", AGENT_ID, "--text", "go"], "steer"),
            (["agents"], "list"),
            (["summary", "--agent-id", AGENT_ID], "summary"),
            (["answer", AGENT_ID], "answer"),
            (["models"], "models"),
            (["limits"], "limits"),
            (["context", "--session-transport", "codex_queue", "--session-id", "s"], "context"),
            (["capacity", "collect", "--once"], "capacity_collect"),
            (["delivery", "status", AGENT_ID], "delivery_status"),
            (["delivery", "cancel", "delivery-1"], "delivery_cancel"),
            (["delivery", "dispatch"], "delivery_dispatch"),
            (["init"], "init"),
            (["doctor"], "doctor"),
        )
        for argv, expected in cases:
            with self.subTest(command=argv):
                code, output, error = self.run_cli(argv, service=service)
                self.assertEqual((code, error), (0, ""))
                self.assertTrue(output.startswith("{"))
                self.assertIn(expected, service.calls)

        code, output, error = self.run_cli(
            ["hook", "context"], service=service, stdin='{"agent_id":"x"}'
        )
        self.assertEqual((code, error), (0, ""))
        self.assertEqual(
            json.loads(output),
            {
                "hookSpecificOutput": {
                    "hookEventName": "UserPromptSubmit",
                    "additionalContext": "context",
                }
            },
        )

        code, output, error = self.run_cli(
            ["hook", "bind"], service=service, stdin='{"agent_id":"x"}'
        )
        self.assertEqual((code, error), (0, ""))
        self.assertEqual(json.loads(output), {"agent_id": "x"})

    def test_capacity_launchd_renders_config_without_state_or_collection(self):
        import plistlib

        with tempfile.TemporaryDirectory() as directory:
            home = Path(directory).resolve()
            config_path = home / "config.toml"
            config_path.write_text(
                "schema_version = 1\n[capacity]\ncollect_interval_seconds = 17\n",
                encoding="utf-8",
            )
            binary = home / "agent&<run>"
            args = [
                "--home",
                str(home),
                "capacity",
                "launchd",
                "--binary",
                str(binary),
            ]

            with patch.object(cli, "collect_once") as collect_once:
                code, output, error = self.run_cli(args)

            self.assertEqual((code, error), (0, ""))
            collect_once.assert_not_called()
            self.assertFalse((home / "state.db").exists())
            rendered = json.loads(output)
            self.assertEqual(
                set(rendered), {"argv", "interval_seconds", "label", "plist"}
            )
            self.assertEqual(rendered["label"], "com.pluto.agent-run.capacity")
            self.assertEqual(rendered["interval_seconds"], 17)
            self.assertEqual(
                rendered["argv"], [str(binary), "capacity", "collect", "--once"]
            )
            parsed = plistlib.loads(rendered["plist"].encode("utf-8"))
            self.assertEqual(parsed["ProgramArguments"], rendered["argv"])
            self.assertEqual(parsed["StartInterval"], 17)
            self.assertEqual(parsed["StandardOutPath"], "/dev/null")
            self.assertEqual(
                parsed["StandardErrorPath"], str(home / "capacity-worker.err.log")
            )
            self.assertIs(parsed["RunAtLoad"], False)
            self.assertNotIn("KeepAlive", parsed)

            config_path.write_text(
                "schema_version = 1\n[capacity]\ncollect_interval_seconds = 19\n",
                encoding="utf-8",
            )
            stdout_log = home / "capacity<&out.log"
            stderr_log = home / "capacity&err.log"
            code, output, error = self.run_cli(
                [
                    *args,
                    "--label",
                    "com.example.<capacity&>",
                    "--stdout-log",
                    str(stdout_log),
                    "--stderr-log",
                    str(stderr_log),
                ]
            )
            self.assertEqual((code, error), (0, ""))
            rendered = json.loads(output)
            parsed = plistlib.loads(rendered["plist"].encode("utf-8"))
            self.assertEqual(rendered["interval_seconds"], 19)
            self.assertEqual(parsed["Label"], "com.example.<capacity&>")
            self.assertEqual(parsed["StandardOutPath"], str(stdout_log))
            self.assertEqual(parsed["StandardErrorPath"], str(stderr_log))
            self.assertNotIn("KeepAlive", parsed)

            code, output, error = self.run_cli(
                ["--home", str(home), "capacity", "launchd", "--binary", "agent-run"]
            )
            self.assertEqual((code, output), (2, ""))
            self.assertEqual(json.loads(error)["error"]["type"], "ValidationError")

    def test_delivery_launchd_renders_a_durable_bounded_sweeper(self):
        import plistlib

        with tempfile.TemporaryDirectory() as directory:
            home = Path(directory).resolve()
            queue = home / "queue"
            (home / "config.toml").write_text(
                f"schema_version = 1\n[delivery]\nretry_base_seconds = 2.1\n"
                f'codex_queue_bin = "{queue}"\n',
                encoding="utf-8",
            )
            binary = home / "agent-run"
            stdout = io.StringIO()
            stderr = io.StringIO()
            code = cli.main(
                [
                    "--home", str(home), "delivery", "launchd", "--binary", str(binary)
                ],
                stdin=io.StringIO(),
                stdout=stdout,
                stderr=stderr,
            )

            self.assertEqual((code, stderr.getvalue()), (0, ""))
            self.assertFalse((home / "state.db").exists())
            rendered = json.loads(stdout.getvalue())
            self.assertEqual(rendered["interval_seconds"], 3)
            self.assertEqual(
                rendered["argv"],
                [str(binary), "--home", str(home), "delivery", "dispatch"],
            )
            parsed = plistlib.loads(rendered["plist"].encode("utf-8"))
            self.assertEqual(parsed["ProgramArguments"], rendered["argv"])
            self.assertEqual(parsed["StartInterval"], 3)
            self.assertIs(parsed["RunAtLoad"], False)
            self.assertNotIn("KeepAlive", parsed)

    def test_init_bootstraps_private_minimal_home_without_credentials(self):
        with tempfile.TemporaryDirectory() as directory:
            home = Path(directory).resolve() / "fresh"
            stdout = io.StringIO()
            stderr = io.StringIO()
            args = ["--home", str(home), "init"]
            code = cli.main(
                args, stdin=io.StringIO(), stdout=stdout, stderr=stderr
            )
            self.assertEqual((code, stderr.getvalue()), (0, ""))
            config = home / "config.toml"
            state = home / "state.db"
            self.assertEqual(config.read_text(encoding="utf-8"), "schema_version = 1\n")
            self.assertTrue(state.is_file())
            self.assertEqual(home.stat().st_mode & 0o777, 0o700)
            self.assertEqual(config.stat().st_mode & 0o777, 0o600)
            self.assertEqual(state.stat().st_mode & 0o777, 0o600)
            self.assertNotIn("token", config.read_text(encoding="utf-8").lower())

            before = config.stat().st_ino
            self.assertEqual(
                cli.main(args, stdin=io.StringIO(), stdout=io.StringIO(), stderr=io.StringIO()),
                0,
            )
            self.assertEqual(config.stat().st_ino, before)

    def test_service_start_renders_the_proven_descriptor_without_secrets(self):
        from agent_run.adapters.opencode import service as opencode_service

        with tempfile.TemporaryDirectory() as directory:
            home = Path(directory).resolve()
            runtime_home = home / "runtimes" / "opencode" / "home"
            runtime_home.mkdir(parents=True)
            binary = home / "opencode2"
            binary.write_text("#!/bin/sh\n", encoding="utf-8")
            (home / "config.toml").write_text(
                "schema_version = 1\n"
                "[runtimes.opencode]\n"
                "enabled = true\n"
                'adapter = "agent_run.adapters.opencode.adapter:ADAPTER"\n'
                f'binary = "{binary}"\n'
                f'home = "{runtime_home}"\n'
                'models = ["omniroute/deepseek-v4-pro"]\n'
                'service_mode = "managed"\n',
                encoding="utf-8",
            )
            started = opencode_service.ServiceStart(
                opencode_service.ServiceDescriptor(
                    host="127.0.0.1",
                    port=41999,
                    config_home=runtime_home / "xdg" / "config",
                    data_home=runtime_home / "xdg" / "data",
                    pid=4242,
                    config_hash="a" * 64,
                    version="2.1.0",
                ),
                False,
            )

            def run(argv):
                stdout = io.StringIO()
                stderr = io.StringIO()
                code = cli.main(
                    ["--home", str(home), "service", *argv],
                    stdin=io.StringIO(),
                    stdout=stdout,
                    stderr=stderr,
                )
                return code, stdout.getvalue(), stderr.getvalue()

            with patch.object(
                opencode_service, "start_service", return_value=started
            ) as start:
                code, output, error = run(["start", "--runtime", "opencode"])
            self.assertEqual((code, error), (0, ""))
            payload = json.loads(output)
            self.assertEqual(payload["runtime"], "opencode")
            self.assertFalse(payload["reused"])
            self.assertEqual(payload["service"]["port"], 41999)
            self.assertEqual(payload["service"]["pid"], 4242)
            self.assertEqual(payload["service"]["version"], "2.1.0")
            self.assertNotIn("password", output.lower())
            runtime, passed_home = start.call_args.args
            self.assertEqual((passed_home, runtime.binary), (runtime_home, binary))
            self.assertIsNone(start.call_args.kwargs["port"])

            with patch.object(
                opencode_service, "start_service", return_value=started
            ) as explicit:
                code, _output, error = run(
                    ["start", "--runtime", "opencode", "--port", "41999"]
                )
            self.assertEqual((code, error), (0, ""))
            self.assertEqual(explicit.call_args.kwargs["port"], 41999)

            code, _output, error = run(["start", "--runtime", "codex"])
            self.assertEqual(code, 2)
            self.assertIn("opencode", json.loads(error)["error"]["message"])

    def test_doctor_delegates_to_the_structured_read_only_seam(self):
        with tempfile.TemporaryDirectory() as directory:
            home = Path(directory).resolve()
            stdout = io.StringIO()
            report = {"home": home, "findings": []}
            with patch.object(cli, "run_doctor", return_value=report) as doctor:
                code = cli.main(
                    ["--home", str(home), "doctor"],
                    stdin=io.StringIO(),
                    stdout=stdout,
                    stderr=io.StringIO(),
                )
            self.assertEqual(code, 0)
            self.assertEqual(
                json.loads(stdout.getvalue()),
                {"home": str(home), "findings": []},
            )
            doctor.assert_called_once_with(home)

    def test_hook_context_wraps_first_injection_and_suppresses_unchanged(self):
        with tempfile.TemporaryDirectory() as directory:
            home = Path(directory).resolve()
            (home / "config.toml").write_text("schema_version = 1\n", encoding="utf-8")
            store = cli.StateStore.initialize(home / "state.db")
            store.close()
            runtime = object.__new__(cli._Runtime)
            runtime.home = home
            payload = json.dumps(
                {
                    "transport": "codex_queue",
                    "external_session_id": "session-1",
                }
            )
            args = ["--home", str(home), "hook", "context"]

            code, output, error = self.run_cli(args, service=runtime, stdin=payload)

            self.assertEqual((code, error), (0, ""))
            envelope = json.loads(output)
            self.assertEqual(set(envelope), {"hookSpecificOutput"})
            hook_output = envelope["hookSpecificOutput"]
            self.assertEqual(
                set(hook_output), {"hookEventName", "additionalContext"}
            )
            self.assertEqual(hook_output["hookEventName"], "UserPromptSubmit")
            self.assertTrue(hook_output["additionalContext"].strip())
            self.assertLessEqual(len(hook_output["additionalContext"]), 2500)

            code, output, error = self.run_cli(args, service=runtime, stdin=payload)

            self.assertEqual((code, error), (0, ""))
            self.assertEqual(json.loads(output), {})
            check_store = cli.StateStore.open(home / "state.db")
            try:
                receipt_count = check_store.connection.execute(
                    "SELECT COUNT(*) FROM context_receipts"
                ).fetchone()[0]
            finally:
                check_store.close()
            self.assertEqual(receipt_count, 1)

    def test_raw_codex_hooks_normalize_context_bind_and_refuse_bad_ids(self):
        from agent_run.domain import AgentStatus, Outcome

        with tempfile.TemporaryDirectory() as directory:
            home = Path(directory).resolve()
            workdir = home / "work"
            workdir.mkdir()
            (home / "config.toml").write_text("schema_version = 1\n", encoding="utf-8")
            store = cli.StateStore.initialize(home / "state.db")
            agent_id = str(
                store.create_agent(
                    StartRequest(
                        "codex", "model", "profile", "task", workdir,
                        timeout_seconds=480,
                    ),
                    task_summary="summary",
                    config_revision="cfg-1",
                    at=1,
                ).agent_id
            )
            store.transition(agent_id, AgentStatus.STARTING, at=2)
            store.transition(agent_id, AgentStatus.RUNNING, at=3)
            store.close()
            runtime = object.__new__(cli._Runtime)
            runtime.home = home
            context_payload = {
                "hook_event_name": "UserPromptSubmit",
                "session_id": "raw-session",
                "turn_id": "turn-1",
                "cwd": str(home),
                "prompt": "must-not-be-logged",
                "transcript_path": "/ignored/raw-transcript",
                "permission_mode": "default",
            }

            with patch.object(
                cli, "build_context", wraps=cli.build_context
            ) as build_context:
                code, output, error = self.run_cli(
                    ["--home", str(home), "hook", "context"],
                    service=runtime,
                    stdin=json.dumps(context_payload),
                )

            self.assertEqual((code, error), (0, ""))
            build_context.assert_called_once()
            ref = build_context.call_args.args[1]
            self.assertEqual(
                (ref.transport, ref.external_session_id, ref.external_turn_id),
                (cli.TRANSPORT_NAME, "raw-session", "turn-1"),
            )
            self.assertNotIn("must-not-be-logged", output)
            self.assertEqual(
                json.loads(output)["hookSpecificOutput"]["hookEventName"],
                "UserPromptSubmit",
            )

            bind_payload = {
                "hook_event_name": "PostToolUse",
                "session_id": "raw-session",
                "turn_id": "turn-2",
                "tool_name": "mcp__agent_run__start",
                "tool_use_id": "call-1",
                "unrelated_metadata": {"ignored": True},
                "tool_response": {
                    "content": [{"type": "text", "text": "ignored"}],
                    "structuredContent": {"agent_id": agent_id},
                },
            }
            # The bind hook fires while the agent is still running, so the
            # terminal transition below creates a deliverable pending notice.
            with patch.object(cli, "run_hook", wraps=cli.run_hook) as run_hook:
                code, output, error = self.run_cli(
                    ["--home", str(home), "hook", "bind"],
                    service=runtime,
                    stdin=json.dumps(bind_payload),
                )

            self.assertEqual((code, error), (0, ""))
            run_hook.assert_called_once()
            self.assertEqual(
                run_hook.call_args.args[1],
                {
                    "agent_id": agent_id,
                    "transport": cli.TRANSPORT_NAME,
                    "external_session_id": "raw-session",
                    "external_turn_id": "turn-2",
                },
            )
            bind_envelope = json.loads(output)
            self.assertEqual(set(bind_envelope), {"hookSpecificOutput"})
            bind_output = bind_envelope["hookSpecificOutput"]
            self.assertEqual(
                set(bind_output), {"hookEventName", "additionalContext"}
            )
            self.assertEqual(bind_output["hookEventName"], "PostToolUse")
            self.assertIn(agent_id, bind_output["additionalContext"])
            self.assertIn("completion will be delivered", bind_output["additionalContext"])

            store = cli.StateStore.open(home / "state.db")
            try:
                store.transition(
                    agent_id,
                    AgentStatus.SUCCEEDED,
                    outcome=Outcome(AgentStatus.SUCCEEDED),
                    at=4,
                )
            finally:
                store.close()
            check_store = cli.StateStore.open(home / "state.db")
            try:
                session = check_store.connection.execute(
                    "SELECT * FROM orchestrator_sessions WHERE external_session_id = ?",
                    ("raw-session",),
                ).fetchone()
                delivery = check_store.connection.execute(
                    "SELECT * FROM deliveries WHERE agent_id = ?", (agent_id,)
                ).fetchone()
                claimed = check_store.claim_delivery("worker", at=10_000_000_000)
            finally:
                check_store.close()
            self.assertEqual(cli.TRANSPORT_NAME, "codex_queue")
            self.assertEqual(session["transport"], cli.TRANSPORT_NAME)
            self.assertEqual(session["external_turn_id"], "turn-2")
            self.assertEqual(delivery["orchestrator_session_id"], session["id"])
            self.assertEqual(delivery["state"], "pending")
            self.assertEqual(claimed["transport"], cli.TRANSPORT_NAME)

            rebind_payload = dict(bind_payload, session_id="another-session")
            code, output, error = self.run_cli(
                ["--home", str(home), "hook", "bind"],
                service=runtime,
                stdin=json.dumps(rebind_payload),
            )
            self.assertEqual((code, output), (2, ""))
            rebind_error = json.loads(error)["error"]
            self.assertEqual(rebind_error["type"], "BindHookError")
            self.assertIn("immutable", rebind_error["message"])

            # Claude Code's MCP client may drop/mis-key structuredContent and
            # carry the id only as JSON text inside a content block. Rebinding
            # the same (agent_id, transport, session) target is idempotent, so
            # this doubles as proof the text-content variant alone is enough
            # to resolve the agent_id.
            text_variant_payload = dict(
                bind_payload,
                tool_response={
                    "structuredContent": {"agentId": agent_id},
                    "content": [{"type": "text", "text": f'{{"agent_id":"{agent_id}"}}'}],
                },
            )
            code, output, error = self.run_cli(
                ["--home", str(home), "hook", "bind"],
                service=runtime,
                stdin=json.dumps(text_variant_payload),
            )
            self.assertEqual((code, error), (0, ""))
            self.assertIn(
                agent_id,
                json.loads(output)["hookSpecificOutput"]["additionalContext"],
            )

            # A real Claude Code PostToolUse payload may serialize the whole
            # MCP tool result as one JSON string in tool_response, rather than
            # a dict or content-block list; that shape alone must still bind.
            string_variant_payload = dict(
                bind_payload,
                tool_response=json.dumps(
                    {"agent_id": agent_id, "created": True, "agent": {"status": "starting"}}
                ),
            )
            code, output, error = self.run_cli(
                ["--home", str(home), "hook", "bind"],
                service=runtime,
                stdin=json.dumps(string_variant_payload),
            )
            self.assertEqual((code, error), (0, ""))
            self.assertIn(
                agent_id,
                json.loads(output)["hookSpecificOutput"]["additionalContext"],
            )

            conflicting_id = agent_id[:-1] + ("0" if agent_id[-1] != "0" else "1")
            refused = {
                "missing": {
                    "structuredContent": {"agentId": agent_id},
                    "content": [{"type": "text", "text": "not json"}],
                },
                "conflicting": [
                    {"structuredContent": {"agent_id": agent_id}},
                    {"agent_id": conflicting_id},
                ],
                "not_json_string": "plain text, not json at all",
            }
            with patch.object(cli, "run_hook", wraps=cli.run_hook) as run_hook:
                for label, tool_response in refused.items():
                    with self.subTest(label=label):
                        payload = dict(bind_payload, tool_response=tool_response)
                        code, output, error = self.run_cli(
                            ["--home", str(home), "hook", "bind"],
                            service=runtime,
                            stdin=json.dumps(payload),
                        )
                        self.assertEqual((code, output), (2, ""))
                        self.assertEqual(
                            json.loads(error)["error"]["type"], "ValidationError"
                        )
                run_hook.assert_not_called()

            for hook, payload in (
                ("context", dict(context_payload, hook_event_name="PostToolUse")),
                ("bind", dict(bind_payload, hook_event_name="UserPromptSubmit")),
            ):
                with self.subTest(mismatched_event=hook):
                    code, output, error = self.run_cli(
                        ["--home", str(home), "hook", hook],
                        service=runtime,
                        stdin=json.dumps(payload),
                    )
                    self.assertEqual((code, output), (2, ""))
                    self.assertEqual(
                        json.loads(error)["error"]["type"], "ValidationError"
                    )

            code, output, error = self.run_cli(
                ["--home", str(home), "hook", "context"],
                service=runtime,
                stdin=json.dumps({"session_id": " "}),
            )
            self.assertEqual((code, output), (2, ""))
            self.assertEqual(json.loads(error)["error"]["type"], "ValidationError")

            normalized = {
                "transport": cli.TRANSPORT_NAME,
                "external_session_id": "strict-normalized",
                "unrelated": True,
            }
            code, output, error = self.run_cli(
                ["--home", str(home), "hook", "context"],
                service=runtime,
                stdin=json.dumps(normalized),
            )
            self.assertEqual((code, output), (2, ""))
            self.assertEqual(json.loads(error)["error"]["type"], "ValidationError")

    def test_mcp_command_is_reserved_without_importing_parallel_module(self):
        sys.modules.pop("agent_run.mcp", None)
        args = cli._parser().parse_args(["mcp"])
        self.assertEqual(args.command, "mcp")
        self.assertNotIn("agent_run.mcp", sys.modules)

    def test_mcp_uses_injected_stdio_for_initialize_and_tools_list(self):
        requests = (
            {"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {}},
            {"jsonrpc": "2.0", "id": 2, "method": "tools/list"},
        )
        stdin = io.StringIO("".join(json.dumps(request) + "\n" for request in requests))
        stdout = io.StringIO()
        stderr = io.StringIO()
        code = cli.main(
            ["mcp"],
            service=FakeService(),
            stdin=stdin,
            stdout=stdout,
            stderr=stderr,
        )
        responses = [json.loads(line) for line in stdout.getvalue().splitlines()]
        self.assertEqual((code, stderr.getvalue()), (0, ""))
        self.assertEqual([response["id"] for response in responses], [1, 2])
        self.assertEqual(
            [tool["name"] for tool in responses[1]["result"]["tools"]],
            [
                "start", "fast", "cancel", "steer", "status", "list_agents",
                "summary", "transcript", "answer", "models", "limits", "doc",
                "workflow_start", "workflow_status", "workflow_cancel", "workflow_answer",
                "workflow_resume",
            ],
        )

    def test_dispatch_composes_sender_transport_and_fresh_store_once(self):
        with tempfile.TemporaryDirectory() as directory:
            home = Path(directory).resolve()
            executable = home / "codex"
            executable.write_text("#!/bin/sh\n", encoding="utf-8")
            executable.chmod(0o700)
            store = Mock()
            sender = Mock()
            transport = Mock()
            dispatcher = Mock()
            result = object()
            dispatcher.run.return_value = result
            config = SimpleNamespace(
                delivery=SimpleNamespace(codex_queue_bin=executable)
            )
            uds_sender = Mock()
            uds_transport = Mock()
            with patch.dict(os.environ, {}, clear=True), patch.object(
                cli, "load_config", return_value=config
            ), patch.object(cli.StateStore, "open", return_value=store) as opened, patch.object(
                cli, "CodexQueueSender", return_value=sender
            ) as sender_type, patch.object(
                cli, "CodexQueueTransport", return_value=transport
            ) as transport_type, patch.object(
                cli, "ClaudeSessionSender", return_value=uds_sender
            ) as uds_sender_type, patch.object(
                cli, "ClaudeUdsTransport", return_value=uds_transport
            ) as uds_transport_type, patch.object(
                cli, "DeliveryDispatcher", return_value=dispatcher
            ) as dispatcher_type:
                self.assertIs(cli._dispatch_once(home), result)

            opened.assert_called_once_with(home / "state.db")
            sender_type.assert_called_once_with(str(executable), timeout_seconds=30.0)
            transport_type.assert_called_once_with(sender)
            uds_sender_type.assert_called_once_with()
            uds_transport_type.assert_called_once_with(uds_sender)
            dispatcher_type.assert_called_once_with(
                store,
                {
                    cli.TRANSPORT_NAME: transport,
                    cli.CLAUDE_UDS_TRANSPORT_NAME: uds_transport,
                },
                config.delivery,
            )
            dispatcher.run.assert_called_once_with(home=home)
            store.close.assert_called_once_with()

            runtime = object.__new__(cli._Runtime)
            runtime.home = home
            with patch.object(cli, "_dispatch_once", return_value=result) as dispatch:
                self.assertIs(runtime.delivery_dispatch(), result)
            dispatch.assert_called_once_with(home)

    def test_dispatch_environment_binary_explicitly_overrides_owner_config(self):
        with tempfile.TemporaryDirectory() as directory:
            home = Path(directory).resolve()
            configured = home / "configured"
            override = home / "override"
            override.write_text("#!/bin/sh\n", encoding="utf-8")
            override.chmod(0o700)
            config = SimpleNamespace(
                delivery=SimpleNamespace(codex_queue_bin=configured)
            )
            dispatcher = Mock()
            dispatcher.run.return_value = object()
            with patch.dict(
                os.environ, {"CODEX_QUEUE_BIN": str(override)}, clear=True
            ), patch.object(cli, "load_config", return_value=config), patch.object(
                cli.StateStore, "open", return_value=Mock()
            ), patch.object(cli, "CodexQueueSender", return_value=Mock()) as sender, patch.object(
                cli, "CodexQueueTransport", return_value=Mock()
            ), patch.object(cli, "DeliveryDispatcher", return_value=dispatcher):
                cli._dispatch_once(home)
            sender.assert_called_once_with(str(override), timeout_seconds=30.0)

    def test_hook_transport_is_per_runtime_and_dispatch_routes_by_the_recorded_name(self):
        from agent_run.delivery.base import DeliveryReceipt
        from agent_run.delivery.dispatch import DeliveryDispatcher
        from agent_run.domain import Outcome

        for command in ("context", "bind"):
            args = cli._parser().parse_args(["hook", command])
            self.assertEqual(args.transport, cli.TRANSPORT_NAME)
            args = cli._parser().parse_args(
                ["hook", command, "--transport", cli.CLAUDE_UDS_TRANSPORT_NAME]
            )
            self.assertEqual(args.transport, cli.CLAUDE_UDS_TRANSPORT_NAME)
            with self.assertRaises(ValidationError):
                cli._parser().parse_args(["hook", command, "--transport", "slack"])
        with self.assertRaises(ValidationError):
            cli._hook_payload({"session_id": "s"}, bind=False, transport="slack")

        with tempfile.TemporaryDirectory() as directory:
            home = Path(directory).resolve()
            workdir = home / "work"
            workdir.mkdir()
            (home / "config.toml").write_text("schema_version = 1\n", encoding="utf-8")
            store = cli.StateStore.initialize(home / "state.db")
            agent_id = str(
                store.create_agent(
                    StartRequest(
                        "claude", "model", "profile", "task", workdir,
                        timeout_seconds=480,
                    ),
                    task_summary="summary",
                    config_revision="cfg-1",
                    at=1,
                ).agent_id
            )
            store.transition(agent_id, AgentStatus.STARTING, at=2)
            store.transition(agent_id, AgentStatus.RUNNING, at=3)
            store.close()
            runtime = object.__new__(cli._Runtime)
            runtime.home = home
            # The bind hook fires while the agent is still running, so the
            # terminal transition below creates a deliverable pending notice.
            code, _output, error = self.run_cli(
                [
                    "--home", str(home), "hook", "bind",
                    "--transport", cli.CLAUDE_UDS_TRANSPORT_NAME,
                ],
                service=runtime,
                stdin=json.dumps(
                    {
                        "hook_event_name": "PostToolUse",
                        "session_id": "claude-session",
                        "turn_id": "turn-9",
                        "tool_response": {"structuredContent": {"agent_id": agent_id}},
                    }
                ),
            )
            self.assertEqual((code, error), (0, ""))

            store = cli.StateStore.open(home / "state.db")
            try:
                store.transition(
                    agent_id,
                    AgentStatus.SUCCEEDED,
                    outcome=Outcome(AgentStatus.SUCCEEDED),
                    at=4,
                )
            finally:
                store.close()
            check_store = cli.StateStore.open(home / "state.db")
            try:
                session = check_store.connection.execute(
                    "SELECT * FROM orchestrator_sessions WHERE external_session_id = ?",
                    ("claude-session",),
                ).fetchone()
                self.assertEqual(session["transport"], cli.CLAUDE_UDS_TRANSPORT_NAME)

                codex = Mock(
                    name="codex", api_version=1, **{"validate.return_value": None}
                )
                claude = Mock(
                    name="claude", api_version=1, **{"validate.return_value": None}
                )
                claude.send.return_value = DeliveryReceipt()
                DeliveryDispatcher(
                    check_store,
                    {
                        cli.TRANSPORT_NAME: codex,
                        cli.CLAUDE_UDS_TRANSPORT_NAME: claude,
                    },
                    owner="router",
                ).drain(at=10_000_000_000)
            finally:
                check_store.close()
            codex.send.assert_not_called()
            claude.send.assert_called_once()
            self.assertEqual(
                claude.send.call_args.args[0].transport,
                cli.CLAUDE_UDS_TRANSPORT_NAME,
            )

    def test_dispatch_requires_an_absolute_codex_queue_binary(self):
        config = SimpleNamespace(
            delivery=SimpleNamespace(codex_queue_bin=None)
        )
        with patch.object(cli, "load_config", return_value=config), patch.dict(
            os.environ, {}, clear=True
        ):
            with self.assertRaisesRegex(ValidationError, "CODEX_QUEUE_BIN"):
                cli._dispatch_once(Path("/tmp/home"))
        with patch.object(cli, "load_config", return_value=config), patch.dict(
            os.environ, {"CODEX_QUEUE_BIN": "codex"}, clear=True
        ):
            with self.assertRaisesRegex(ValidationError, "absolute executable"):
                cli._dispatch_once(Path("/tmp/home"))

    def test_launch_hands_over_one_exec_payload_and_reconciles_on_reap(self):
        with tempfile.TemporaryDirectory() as directory:
            home = Path(directory)
            workdir = home / "work"
            workdir.mkdir()
            request = StartRequest(
                "codex", "model", "review", "task", workdir,
                timeout_seconds=480,
            )
            store = Mock()
            plan = Mock()
            plan.to_payload.return_value = {"argv": ["engine"], "environment": {"T": "s"}}
            events = []
            captured = {}
            store.close.side_effect = lambda: events.append("store_closed")

            def reconciled(reconcile_store, reconciled_agent_id, pid):
                self.assertIs(reconcile_store, store)
                self.assertEqual(reconciled_agent_id, AgentId(AGENT_ID))
                self.assertEqual(pid, 123)
                events.append("reconcile")

            def detached(payload, **kwargs):
                captured.update(payload=payload, kwargs=kwargs)
                kwargs["post_reap"](123, 0)
                return 123

            with patch.object(cli.StateStore, "open", return_value=store) as opened, patch.object(
                cli, "load_config", return_value=SimpleNamespace(core=SimpleNamespace(warning_fraction=0.9, stalled_after_seconds=900.0))
            ), patch.object(
                cli, "launch_detached", side_effect=detached
            ), patch.object(
                cli, "reconcile_reaped_agent", side_effect=reconciled
            ):
                cli._launch_callback(home)(
                    AgentId(AGENT_ID), request, object(), plan, home / "agents" / AGENT_ID
                )

        # The supervisor now runs in an exec'd interpreter, so the parent must
        # hand over data only: no callable, no adapter, no open store.
        opened.assert_called_once_with(home.resolve() / "state.db")
        self.assertEqual(events, ["reconcile", "store_closed"])
        self.assertEqual(captured["kwargs"]["executable"], sys.executable)
        self.assertEqual(captured["kwargs"]["post_terminal_timeout_seconds"], 31.0)
        self.assertEqual(
            json.loads(json.dumps(captured["payload"])),
            {
                "agent_id": AGENT_ID,
                "home": str(home),
                "runtime": "codex",
                "timeout_seconds": 480,
                "answer_path": str(home / "agents" / AGENT_ID / "answer.md"),
                "agent_dir": str(home / "agents" / AGENT_ID),
                "warning_fraction": 0.9,
                "stalled_after_seconds": 900.0,
                "plan": {"argv": ["engine"], "environment": {"T": "s"}},
            },
        )


class PackagingTests(unittest.TestCase):
    def test_console_script_and_schema_are_present_in_sdist(self):
        root = Path(__file__).resolve().parents[1]
        config = tomllib.loads((root / "pyproject.toml").read_text(encoding="utf-8"))
        self.assertEqual(config["project"]["scripts"]["agent-run"], "agent_run.cli:main")
        self.assertIn("schema.sql", config["tool"]["setuptools"]["package-data"]["agent_run.state"])

        with tempfile.TemporaryDirectory() as directory:
            temporary = Path(directory)
            project = temporary / "project"
            project.mkdir()
            shutil.copy2(root / "pyproject.toml", project / "pyproject.toml")
            shutil.copytree(root / "src", project / "src")
            distribution = temporary / "dist"
            distribution.mkdir()
            previous = Path.cwd()
            try:
                os.chdir(project)
                try:
                    from setuptools.build_meta import build_sdist
                except ModuleNotFoundError:
                    self.skipTest("setuptools is not installed; pyproject metadata was verified")

                with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
                    archive = distribution / build_sdist(str(distribution))
            finally:
                os.chdir(previous)
            with tarfile.open(archive, "r:gz") as bundle:
                names = bundle.getnames()
                self.assertTrue(any(name.endswith("/agent_run/state/schema.sql") for name in names))
                entry = next(name for name in names if name.endswith(".egg-info/entry_points.txt"))
                metadata = bundle.extractfile(entry).read().decode("utf-8")
        self.assertIn("agent-run = agent_run.cli:main", metadata)


if __name__ == "__main__":
    unittest.main()
