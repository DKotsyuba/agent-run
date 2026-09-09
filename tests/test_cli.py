import contextlib
import io
import json
import os
import shutil
import sys
import tarfile
import tempfile
import time
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
from agent_run.preparation import request_payload
from agent_run.role_plan import ResolvedRolePlan
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

    def resume(self, agent_id, task, **kwargs):
        """Capture the continuation request without starting an agent."""
        self.request = {"agent_id": agent_id, "task": task, **kwargs}
        return self._return("resume", FakeStart())

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
        return self._return(
            "answer",
            FakeView(
                AgentStatus.RUNNING,
                Path("/tmp/answer.md"),
                MappingProxyType({"agent_id": agent_id}),
            ),
        )

    def capacity_collect(self):
        return self._return("capacity_collect", {"collected": True})


    def init(self):
        return self._return("init", {"initialized": True})

    def doctor(self):
        return self._return("doctor", {"healthy": True})


class CliTests(unittest.TestCase):
    def test_resume_task_file_preserves_whitespace_and_session_binding(self):
        """Task-file input remains exact and belongs to the new notification caller."""
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "task.txt"
            path.write_bytes(b"  fix this\r\n\r\n")
            service = FakeService()
            code, output, error = self.run_cli([
                "resume", AGENT_ID, "--task-file", str(path), "--timeout", "32",
                "--request-id", "retry", "--session-transport", "codex_queue",
                "--session-id", "new-caller",
            ], service=service)
            self.assertEqual((code, error), (0, ""))
            self.assertEqual(json.loads(output)["agent_id"], AGENT_ID)
            self.assertEqual(service.request["task"], "  fix this\r\n\r\n")
            self.assertEqual(service.request["orchestrator"].external_session_id, "new-caller")
            self.assertEqual(service.request["timeout_seconds"], 32)
        self.assertEqual(self.run_cli(["resume", AGENT_ID, "--task", "a", "--task-file", "b"])[0], 2)

    def test_resume_uses_broker_without_constructing_local_runtime(self):
        """A short-lived CLI never owns a continuation preparation worker."""
        broker = Mock()
        broker.resume.return_value = FakeStart()
        with patch.object(cli, "BrokerClient", return_value=broker), patch.object(cli, "_Runtime", side_effect=AssertionError("local runtime")):
            code = cli.main(["resume", AGENT_ID, "--task", "fix"], stdout=io.StringIO(), stderr=io.StringIO())
        self.assertEqual(code, 0)
        broker.resume.assert_called_once()
        self.assertEqual(broker.resume.call_args.args, (AGENT_ID, "fix"))
        broker.close.assert_called_once()


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

    def test_one_shot_start_uses_resident_broker_without_ephemeral_runtime(self):
        with tempfile.TemporaryDirectory() as directory:
            broker = Mock()
            broker.start.return_value = FakeStart()
            output, error = io.StringIO(), io.StringIO()
            with patch.object(cli, "BrokerClient", return_value=broker) as factory:
                with patch.object(cli, "_Runtime") as runtime:
                    code = cli.main(
                        ["--home", directory, "start", "--runtime", "codex",
                         "--model", "model", "--profile", "review", "--task", "task",
                         "--workdir", directory],
                        stdin=io.StringIO(), stdout=output, stderr=error,
                    )
            self.assertEqual(code, 0)
            factory.assert_called_once_with(Path(directory).resolve() / "api.sock")
            runtime.assert_not_called()
            broker.close.assert_called_once_with()
            self.assertIn(AGENT_ID, output.getvalue())

    def test_auth_runs_codex_login_in_account_home(self):
        """Run account login without resolving away the configured launcher path."""
        with tempfile.TemporaryDirectory() as directory:
            home = Path(directory)
            (home / "config.toml").write_text('''schema_version = 1
[runtimes.codex]
enabled = true
adapter = "agent_run.adapters.codex:ADAPTER"
binary = "/bin/codex"
home = "/tmp/codex"
models = ["model"]
accounts = ["personal2"]
[runtimes.codex.auth]
kind = "file_link"
source = "/tmp/auth"
target = "auth.json"
''', encoding="utf-8")
            calls = []
            def run(argv, **kwargs):
                calls.append((argv, kwargs))
                return type("Result", (), {"returncode": 0, "stdout": ""})()
            with patch("agent_run.cli.subprocess.run", side_effect=run):
                stdout = io.StringIO()
                stderr = io.StringIO()
                code = cli.main(
                    ["--home", str(home), "auth", "personal2", "codex"],
                    service=None,
                    stdin=io.StringIO(),
                    stdout=stdout,
                    stderr=stderr,
                )
                output, error = stdout.getvalue(), stderr.getvalue()
        self.assertEqual(code, 0)
        self.assertEqual(json.loads(output), {"account": "personal2", "runtime": "codex", "status": "ok"})
        binary = "/bin/codex"
        self.assertEqual(calls[0][0], [binary, "login"])
        self.assertEqual(calls[1][0], [binary, "login", "status"])
        self.assertEqual(Path(calls[0][1]["env"]["CODEX_HOME"]).resolve(), home.resolve() / "accounts" / "codex" / "personal2")

    def test_login_claude_uses_the_native_global_config_directory(self):
        """An omitted account authenticates the host Claude CLI state."""

        with tempfile.TemporaryDirectory() as directory:
            home = Path(directory)
            runtime_home = home / "runtime-claude"
            (home / "config.toml").write_text(
                f'''schema_version = 1
[runtimes.claude]
enabled = true
adapter = "agent_run.adapters.claude.adapter:ADAPTER"
binary = "/bin/claude"
home = "{runtime_home}"
models = ["sonnet"]
[runtimes.claude.auth]
kind = "environment"
names = ["CLAUDE_CODE_OAUTH_TOKEN"]
''',
                encoding="utf-8",
            )
            calls = []

            def run(argv, **kwargs):
                """Capture the CLI calls without starting a browser flow."""

                calls.append((argv, kwargs))
                return type("Result", (), {"returncode": 0, "stdout": "{}"})()

            with patch.dict(os.environ, {"CLAUDE_CONFIG_DIR": "/ambient", "CLAUDE_CODE_OAUTH_TOKEN": "secret"}), patch(
                "agent_run.cli.subprocess.run", side_effect=run
            ):
                stdout, stderr = io.StringIO(), io.StringIO()
                code = cli.main(
                    ["--home", str(home), "login", "claude"],
                    service=None,
                    stdin=io.StringIO(),
                    stdout=stdout,
                    stderr=stderr,
                )
        self.assertEqual(code, 0)
        self.assertEqual(json.loads(stdout.getvalue()), {"account": None, "runtime": "claude", "status": "ok"})
        self.assertEqual(calls[0][0], ["/bin/claude", "auth", "login"])
        self.assertEqual(calls[1][0], ["/bin/claude", "auth", "status", "--json"])
        environment = calls[0][1]["env"]
        self.assertEqual(environment["CLAUDE_CONFIG_DIR"], "/ambient")
        self.assertNotIn("CLAUDE_CODE_OAUTH_TOKEN", environment)

    def test_login_claude_account_and_status_failure_are_scoped_and_safe(self):
        """Selected accounts get distinct config state and status output stays private."""

        with tempfile.TemporaryDirectory() as directory:
            home = Path(directory)
            (home / "config.toml").write_text(
                '''schema_version = 1
[runtimes.claude]
enabled = true
adapter = "agent_run.adapters.claude.adapter:ADAPTER"
binary = "/bin/claude"
home = "/tmp/claude"
models = ["sonnet"]
accounts = ["personal"]
[runtimes.claude.auth]
kind = "environment"
names = ["CLAUDE_CODE_OAUTH_TOKEN"]
''',
                encoding="utf-8",
            )
            calls = []

            def run(argv, **kwargs):
                """Return an authenticated login followed by a rejected status."""

                calls.append((argv, kwargs))
                return type("Result", (), {"returncode": 0 if len(calls) == 1 else 17, "stdout": "secret status"})()

            with patch("agent_run.cli.subprocess.run", side_effect=run):
                stdout, stderr = io.StringIO(), io.StringIO()
                code = cli.main(
                    ["--home", str(home), "login", "claude", "--account", "personal"],
                    service=None,
                    stdin=io.StringIO(),
                    stdout=stdout,
                    stderr=stderr,
                )
        self.assertEqual(code, 17)
        self.assertEqual(stdout.getvalue(), "")
        self.assertEqual(stderr.getvalue(), "auth login status failed for personal claude (exit 17)\n")
        self.assertEqual(
            calls[0][1]["env"]["CLAUDE_CONFIG_DIR"],
            str(Path("/tmp/claude@personal/claude-config").resolve()),
        )

    def test_login_claude_defaults_global_and_rejects_unsupported_runtime(self):
        """Use native global Claude state and keep other runtime syntax explicit."""

        with tempfile.TemporaryDirectory() as directory:
            home = Path(directory)
            (home / "config.toml").write_text(
                '''schema_version = 1
[runtimes.claude]
enabled = true
adapter = "agent_run.adapters.claude.adapter:ADAPTER"
binary = "/bin/claude"
home = "/tmp/claude"
models = ["sonnet"]
accounts = ["personal"]
[runtimes.claude.auth]
kind = "environment"
names = ["CLAUDE_CODE_OAUTH_TOKEN"]
[runtimes.codex]
enabled = true
adapter = "agent_run.adapters.codex:ADAPTER"
binary = "/bin/codex"
home = "/tmp/codex"
models = ["gpt"]
accounts = ["personal"]
[runtimes.codex.auth]
kind = "file_link"
source = "/tmp/auth"
target = "auth.json"
''',
                encoding="utf-8",
            )
            def command(argv):
                """Run a standalone login command and capture its public result."""

                stdout, stderr = io.StringIO(), io.StringIO()
                return (
                    cli.main(argv, service=None, stdin=io.StringIO(), stdout=stdout, stderr=stderr),
                    stdout.getvalue(),
                    stderr.getvalue(),
                )

            result = type("Result", (), {"returncode": 0, "stdout": "{}"})()
            with patch("agent_run.cli.subprocess.run", return_value=result):
                global_login = command(["--home", str(home), "login", "claude"])
                unsupported = command(["--home", str(home), "login", "codex"])
        self.assertEqual(global_login[0], 0)
        self.assertEqual(json.loads(global_login[1])["account"], None)
        self.assertEqual(unsupported[0], 2)
        self.assertIn("agent-run auth <label> codex", unsupported[2])

    def test_start_account_flag_reaches_request(self):
        with tempfile.TemporaryDirectory() as directory:
            service = FakeService()
            code, _output, _error = self.run_cli(
                ["start", "--runtime", "codex", "--model", "model", "--profile", "p", "--task", "t", "--workdir", directory, "--account", "personal2"],
                service=service,
            )
        self.assertEqual(code, 0)
        self.assertEqual(service.request.account, "personal2")

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
        code, output, error = self.run_cli(["answer", AGENT_ID])
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
        code, output, error = self.run_cli(["answer", AGENT_ID], service=expected)
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
            self.run_cli(["answer", AGENT_ID], service=unexpected)

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

        code, output, error = self.run_cli(["answer", AGENT_ID], service=bootstrapped)
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
            (["cancel", AGENT_ID], "cancel"),
            (["steer", AGENT_ID, "--text", "go"], "steer"),
            (["agents"], "list"),
            (["answer", AGENT_ID], "answer"),
            (["capacity", "collect", "--once"], "capacity_collect"),
            (["init"], "init"),
            (["doctor"], "doctor"),
        )
        for argv, expected in cases:
            with self.subTest(command=argv):
                code, output, error = self.run_cli(argv, service=service)
                self.assertEqual((code, error), (0, ""))
                self.assertTrue(output.startswith("{"))
                self.assertIn(expected, service.calls)

    def test_removed_workflow_commands_are_not_parsed(self):
        """Legacy workflow and batch entry points are absent from the CLI."""

        for command in (
            ["workflow", "status", "wf_old"], ["batch", "--file", "-"],
            ["delivery", "dispatch"], ["hook", "context"], ["bind", AGENT_ID],
            ["context"], ["chain", AGENT_ID], ["status", AGENT_ID],
            ["summary"], ["models"], ["limits"], ["doc"],
        ):
            with self.subTest(command=command), self.assertRaises(ValidationError):
                cli._parser().parse_args(command)

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


    def test_api_launchd_renders_a_keep_alive_resident_daemon(self):
        import plistlib

        with tempfile.TemporaryDirectory() as directory:
            home = Path(directory).resolve()
            binary = home / "agent-run"
            code, output, error = self.run_cli(
                [
                    "--home", str(home), "api", "launchd", "--binary", str(binary)
                ]
            )

            self.assertEqual((code, error), (0, ""))
            rendered = json.loads(output)
            self.assertEqual(rendered["label"], "com.agent-run.api")
            self.assertEqual(
                rendered["argv"],
                [str(binary), "--home", str(home), "api", "serve"],
            )
            parsed = plistlib.loads(rendered["plist"].encode("utf-8"))
            self.assertEqual(parsed["ProgramArguments"], rendered["argv"])
            self.assertIs(parsed["RunAtLoad"], True)
            self.assertIs(parsed["KeepAlive"], True)
            self.assertEqual(parsed["StandardOutPath"], str(home / "logs" / "api.log"))
            self.assertEqual(parsed["StandardErrorPath"], str(home / "logs" / "api.err.log"))

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


    def test_mcp_command_is_reserved_without_importing_parallel_module(self):
        sys.modules.pop("agent_run.mcp", None)
        args = cli._parser().parse_args(["mcp"])
        self.assertEqual(args.command, "mcp")
        self.assertNotIn("agent_run.mcp", sys.modules)

    def test_mcp_uses_injected_stdio_for_initialize_and_tools_list(self):
        """Keep the injected SDK stream alive until its concurrent list reply lands."""

        class _DelayedEofInput(io.StringIO):
            """Delay EOF briefly after all injected protocol frames are consumed."""

            def read(self, size=-1):
                """Return buffered frames, then hold EOF for pending SDK callbacks."""
                value = super().read(size)
                if not value:
                    time.sleep(0.2)
                return value

        requests = (
            {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "initialize",
                "params": {
                    "protocolVersion": "2026-07-28",
                    "capabilities": {},
                    "clientInfo": {"name": "test", "version": "1"},
                },
            },
            {"jsonrpc": "2.0", "method": "notifications/initialized"},
            {"jsonrpc": "2.0", "id": 2, "method": "tools/list"},
        )
        stdin = _DelayedEofInput(
            "".join(json.dumps(request) + "\n" for request in requests)
        )
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
            ["capacity_order", "start", "cancel", "steer", "list_agents",
             "transcript", "answer", "resume"],
        )


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
            role = ResolvedRolePlan.from_payload(
                json.loads(
                    (Path(__file__).parent / "fixtures" / "role_plan_7bbd43b.json").read_text()
                )
            )
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
                cli, "launch_detached", side_effect=detached
            ), patch.object(
                cli, "reconcile_reaped_agent", side_effect=reconciled
            ):
                cli._launch_callback(home)(
                    AgentId(AGENT_ID), request, role
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
                "request": request_payload(request),
                "role": role.to_payload(),
            },
        )


class PackagingTests(unittest.TestCase):
    def test_console_script_and_schema_are_present_in_sdist(self):
        root = Path(__file__).resolve().parents[1]
        config = tomllib.loads((root / "pyproject.toml").read_text(encoding="utf-8"))
        self.assertEqual(config["project"]["scripts"], {"agent-run": "agent_run.cli:main"})
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
        self.assertNotIn("agent-run-tui", metadata)


if __name__ == "__main__":
    unittest.main()
