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

    def hook_context(self, payload):
        return self._return(
            "hook_context",
            {
                "hookSpecificOutput": {
                    "hookEventName": "UserPromptSubmit",
                    "additionalContext": "context",
                }
            },
        )

    def hook_bind(self, payload):
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
        self.assertEqual(request.effort, "high")
        self.assertEqual(request.timeout_seconds, 42)
        self.assertEqual(request.read_roots, (read_root.resolve(),))
        self.assertEqual(request.output_schema, {"type": "object"})
        self.assertEqual(request.request_id, "request-1")
        self.assertEqual(request.orchestrator.external_turn_id, "turn-1")

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
                    StartRequest("codex", "model", "profile", "task", workdir),
                    task_summary="summary",
                    config_revision="cfg-1",
                    at=1,
                ).agent_id
            )
            store.transition(agent_id, AgentStatus.STARTING, at=2)
            store.transition(agent_id, AgentStatus.RUNNING, at=3)
            store.transition(
                agent_id,
                AgentStatus.SUCCEEDED,
                outcome=Outcome(AgentStatus.SUCCEEDED),
                at=4,
            )
            store.close()
            runtime = object.__new__(cli._Runtime)
            runtime.home = home
            context_payload = {
                "hook_event_name": "UserPromptSubmit",
                "session_id": "raw-session",
                "turn_id": "turn-1",
                "cwd": str(home),
                "prompt": "must-not-be-logged",
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
                ("codex", "raw-session", "turn-1"),
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
                "tool_response": {
                    "content": [{"type": "text", "text": "ignored"}],
                    "structuredContent": {"agent_id": agent_id},
                },
            }
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
                    "transport": "codex",
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

            check_store = cli.StateStore.open(home / "state.db")
            try:
                session = check_store.connection.execute(
                    "SELECT * FROM orchestrator_sessions WHERE external_session_id = ?",
                    ("raw-session",),
                ).fetchone()
                delivery = check_store.connection.execute(
                    "SELECT * FROM deliveries WHERE agent_id = ?", (agent_id,)
                ).fetchone()
            finally:
                check_store.close()
            self.assertEqual(session["transport"], "codex")
            self.assertEqual(session["external_turn_id"], "turn-2")
            self.assertEqual(delivery["orchestrator_session_id"], session["id"])
            self.assertEqual(delivery["state"], "pending")

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

            conflicting_id = agent_id[:-1] + ("0" if agent_id[-1] != "0" else "1")
            refused = {
                "missing": {
                    "structuredContent": {"agentId": agent_id},
                    "content": [{"text": f'{{"agent_id":"{agent_id}"}}'}],
                },
                "conflicting": [
                    {"structuredContent": {"agent_id": agent_id}},
                    {"agent_id": conflicting_id},
                ],
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

            code, output, error = self.run_cli(
                ["--home", str(home), "hook", "context"],
                service=runtime,
                stdin=json.dumps({"session_id": " "}),
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
                "start", "cancel", "steer", "status", "list_agents",
                "summary", "transcript", "answer", "models", "limits",
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
            config = SimpleNamespace(delivery=object())
            with patch.dict(os.environ, {"CODEX_QUEUE_BIN": str(executable)}), patch.object(
                cli, "load_config", return_value=config
            ), patch.object(cli.StateStore, "open", return_value=store) as opened, patch.object(
                cli, "CodexQueueSender", return_value=sender
            ) as sender_type, patch.object(
                cli, "CodexQueueTransport", return_value=transport
            ) as transport_type, patch.object(
                cli, "DeliveryDispatcher", return_value=dispatcher
            ) as dispatcher_type:
                self.assertIs(cli._dispatch_once(home), result)

            opened.assert_called_once_with(home / "state.db")
            sender_type.assert_called_once_with(str(executable), timeout_seconds=30.0)
            transport_type.assert_called_once_with(sender)
            dispatcher_type.assert_called_once_with(
                store, {cli.TRANSPORT_NAME: transport}, config.delivery
            )
            dispatcher.run.assert_called_once_with(home=home, max_batch=1)
            store.close.assert_called_once_with()

            runtime = object.__new__(cli._Runtime)
            runtime.home = home
            with patch.object(cli, "_dispatch_once", return_value=result) as dispatch:
                self.assertIs(runtime.delivery_dispatch(), result)
            dispatch.assert_called_once_with(home)

    def test_dispatch_requires_an_absolute_codex_queue_binary(self):
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaisesRegex(ValidationError, "CODEX_QUEUE_BIN"):
                cli._dispatch_once(Path("/tmp/home"))
        with patch.dict(os.environ, {"CODEX_QUEUE_BIN": "codex"}, clear=True):
            with self.assertRaisesRegex(ValidationError, "absolute executable"):
                cli._dispatch_once(Path("/tmp/home"))

    def test_terminal_dispatch_uses_fresh_store_and_never_reruns_agent(self):
        with tempfile.TemporaryDirectory() as directory:
            home = Path(directory)
            workdir = home / "work"
            workdir.mkdir()
            request = StartRequest("codex", "model", "review", "task", workdir)
            store = Mock()
            supervisor = Mock()
            ready = object()
            events = []
            store.close.side_effect = lambda: events.append("store_closed")

            def failed_dispatch(dispatch_home):
                self.assertEqual(dispatch_home, home)
                self.assertTrue(store.close.called)
                events.append("dispatch")
                raise RuntimeError("delivery failed")

            def detached(child, *, post_terminal, post_terminal_timeout_seconds):
                events.append("child")
                child(ready)
                self.assertEqual(post_terminal_timeout_seconds, 31.0)
                try:
                    post_terminal()
                except RuntimeError:
                    events.append("dispatch_failed")
                return 123

            with patch.object(cli.StateStore, "open", return_value=store) as opened, patch.object(
                cli, "load_config", return_value=SimpleNamespace(core=SimpleNamespace(warning_fraction=0.9))
            ), patch.object(cli, "Supervisor", return_value=supervisor) as constructor, patch.object(
                cli, "launch_detached", side_effect=detached
            ), patch.object(
                cli, "_dispatch_once", side_effect=failed_dispatch
            ):
                cli._launch_callback(home)(
                    AgentId(AGENT_ID), request, object(), object(), home / "agents" / AGENT_ID
                )

        opened.assert_called_once_with(home.resolve() / "state.db")
        store.close.assert_called_once_with()
        supervisor.run.assert_called_once_with()
        self.assertEqual(events, ["child", "store_closed", "dispatch", "dispatch_failed"])
        self.assertEqual(constructor.call_args.kwargs["answer_path"], home / "agents" / AGENT_ID / "answer.md")
        self.assertEqual(constructor.call_args.kwargs["timeout_seconds"], 480)
        self.assertIs(constructor.call_args.kwargs["ready"], ready)


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
