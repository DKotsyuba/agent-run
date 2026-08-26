import hashlib
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from agent_run.adapters.base import ADAPTER_API_VERSION, Capability
from agent_run.adapters.home import content_hash
from agent_run.adapters.opencode.adapter import (
    ANSWER_NAME,
    CONFIG_RELATIVE_PATH,
    PRIMARY_AGENT,
    RUNTIME_NAME,
    VERIFY_AGENT,
    OpenCodeAdapter,
    OpenCodeRuntimeSession,
    PermissionBroker,
    extract_answer,
    is_settled,
    is_working,
    model_reference,
    normalize_models,
    normalize_outcome,
    normalize_transcript,
    render_config,
    split_model,
)
from agent_run.adapters.opencode.http import HttpResponse
from agent_run.adapters.opencode.service import (
    PASSWORD_ENV,
    SERVICE_HOST,
    SERVICE_PATH,
    ServiceIsolationError,
    build_service_plan,
    service_home_paths,
    verify_isolation,
    write_service_descriptor,
)
from agent_run.config import McpConfig, RuntimeConfig, RuntimeHookConfig
from agent_run.domain import AgentStatus, MessageRole, StartRequest
from agent_run.errors import ValidationError
from agent_run.profiles import AgentProfile

from test_opencode_service import runtime_config


PORT = 41777
MODEL = "omniroute/deepseek-v4-pro"
ALT_MODEL = "omniroute/minimax-m3"


def message(role, text, *, agent=PRIMARY_AGENT, at=1.0):
    """One v2 transcript entry: metadata in ``info``, content in ``parts``."""

    return {
        "info": {"role": role, "agent": agent, "time": {"created": at}, "sessionID": "ses_1"},
        "parts": [{"type": "text", "text": text}],
    }


class PermissionBrokerTests(unittest.TestCase):
    def setUp(self):
        self._temp = tempfile.TemporaryDirectory()
        self.addCleanup(self._temp.cleanup)
        self.root = Path(self._temp.name).resolve()
        self.allowed = self.root / "allowed"
        (self.allowed / "nested").mkdir(parents=True)
        (self.root / "other").mkdir()
        self.broker = PermissionBroker((self.allowed,))

    def test_contained_external_directory_is_granted_once(self):
        first = self.broker.decide(
            {"id": "p1", "type": "external_directory", "path": str(self.allowed / "nested")}
        )
        self.assertTrue(first.granted)
        self.assertEqual(self.broker.granted_directory, str(self.allowed / "nested"))
        self.assertEqual(dict(self.broker.reply(first)), {"reply": "once"})
        second = self.broker.decide(
            {"id": "p2", "type": "external_directory", "path": str(self.allowed)}
        )
        self.assertFalse(second.granted)
        self.assertIn("already granted once", second.reason)

    def test_reply_body_carries_only_the_reply(self):
        decision = self.broker.decide({"id": "p1", "type": "bash", "path": "/"})
        self.assertEqual(dict(self.broker.reply(decision)), {"reply": "reject"})
        self.assertNotIn("response", self.broker.reply(decision))

    def test_directory_outside_read_roots_is_rejected(self):
        decision = self.broker.decide(
            {"id": "p1", "type": "external_directory", "path": str(self.root / "other")}
        )
        self.assertFalse(decision.granted)
        self.assertIsNone(self.broker.granted_directory)
        self.assertEqual(self.broker.reply(decision)["reply"], "reject")

    def test_every_other_permission_is_auto_rejected(self):
        for kind in ("bash", "edit", "write", "webfetch", None):
            decision = self.broker.decide({"id": f"p-{kind}", "type": kind, "path": "/"})
            self.assertFalse(decision.granted)
        self.assertEqual(
            dict(self.broker.blocked_summary()),
            {"None": 1, "bash": 1, "edit": 1, "webfetch": 1, "write": 1},
        )

    def test_unusable_permission_payloads_are_refused(self):
        with self.assertRaises(ValidationError):
            self.broker.decide({"type": "external_directory", "path": str(self.allowed)})
        relative = self.broker.decide({"id": "p1", "type": "external_directory", "path": "allowed"})
        self.assertFalse(relative.granted)


class TranscriptTests(unittest.TestCase):
    def test_primary_text_is_preserved_across_a_steer_and_a_retry(self):
        payload = [
            message("user", "real task"),
            message("assistant", "part one", at=2.0),
            message("user", "also check the parser", at=3.0),
            message("assistant", "part two", at=4.0),
            message("user", "retrying", at=5.0),
            message("assistant", "part three", at=6.0),
        ]
        self.assertEqual(extract_answer(payload), "part one\n\npart two\n\npart three")

    def test_sub_agent_output_is_excluded_from_the_answer(self):
        payload = {
            "messages": [
                message("user", "task"),
                message("assistant", "verifier notes", agent=VERIFY_AGENT, at=2.0),
                message("assistant", "primary answer", at=3.0),
            ]
        }
        self.assertEqual(extract_answer(payload), "primary answer")
        self.assertEqual(extract_answer(payload, agent=VERIFY_AGENT), "verifier notes")

    def test_transcript_keeps_every_agent_and_drops_empty_text(self):
        payload = [
            message("user", "task"),
            {"info": {"role": "assistant"}, "parts": [{"type": "tool", "tool": "bash"}]},
            message("assistant", "answer", agent=VERIFY_AGENT, at=2.0),
        ]
        messages = normalize_transcript(payload, raw_ref="/tmp/reply.json")
        self.assertEqual([item.role for item in messages], [MessageRole.USER, MessageRole.ASSISTANT])
        self.assertEqual(messages[1].name, VERIFY_AGENT)
        self.assertEqual(messages[1].raw_ref, "/tmp/reply.json")
        self.assertEqual(messages[1].at, 2.0)

    def test_unknown_role_and_malformed_info_are_refused(self):
        with self.assertRaises(ValidationError):
            normalize_transcript([message("tool", "x")])
        with self.assertRaises(ValidationError):
            normalize_transcript([{"info": "assistant", "parts": [{"type": "text", "text": "x"}]}])


class OutcomeTests(unittest.TestCase):
    def test_states_normalize_to_terminal_outcomes(self):
        self.assertEqual(normalize_outcome({"state": "completed"}).status, AgentStatus.SUCCEEDED)
        self.assertEqual(normalize_outcome({"state": "idle"}).status, AgentStatus.SUCCEEDED)
        self.assertEqual(normalize_outcome({"state": "aborted"}).status, AgentStatus.CANCELLED)
        self.assertEqual(normalize_outcome({"state": "timeout"}).status, AgentStatus.TIMED_OUT)

    def test_reported_error_wins_over_a_completed_state(self):
        outcome = normalize_outcome(
            {"state": "idle", "error": {"name": "ProviderError", "message": "429"}},
            runtime_session_id="ses_1",
        )
        self.assertEqual(outcome.status, AgentStatus.FAILED)
        self.assertEqual(outcome.failure_kind, "ProviderError")
        self.assertEqual(outcome.runtime_session_id, "ses_1")

    def test_busy_and_retrying_are_work_not_outcomes(self):
        for state in ("busy", "retrying"):
            self.assertTrue(is_working({"state": state}))
            self.assertFalse(is_settled({"state": state}))
            with self.assertRaises(ValidationError):
                normalize_outcome({"state": state})
        self.assertTrue(is_settled({"state": "idle"}))
        self.assertFalse(is_working({}))


class ModelTests(unittest.TestCase):
    def test_canonical_identifiers_split_into_provider_and_model(self):
        self.assertEqual(split_model(MODEL), ("omniroute", "deepseek-v4-pro"))
        self.assertEqual(
            dict(model_reference(MODEL)), {"providerID": "omniroute", "modelID": "deepseek-v4-pro"}
        )

    def test_non_canonical_identifiers_are_refused(self):
        for value in ("minimax-m3", "a/b/c", "/model", "provider/", 7, None):
            with self.assertRaises(ValidationError):
                split_model(value)

    def test_roster_is_intersected_with_the_allowlist_in_config_order(self):
        payload = {
            "data": [
                {"providerID": "opencode", "id": "grok-4-fast", "enabled": True, "status": "active"},
                {"providerID": "omniroute", "id": "minimax-m3", "enabled": True, "status": "active", "name": "MiniMax M3"},
                {"providerID": "omniroute", "id": "deepseek-v4-pro", "enabled": True, "status": "active"},
                {"providerID": "omniroute", "id": "unlisted", "enabled": True, "status": "active"},
            ],
            "location": {},
        }
        models = normalize_models(payload, (ALT_MODEL, MODEL, "omniroute/absent"))
        self.assertEqual([item.id for item in models], [ALT_MODEL, MODEL])
        self.assertEqual(models[0].description, "MiniMax M3")

    def test_disabled_or_inactive_entries_are_not_reported(self):
        payload = {
            "data": [
                {"providerID": "omniroute", "id": "deepseek-v4-pro", "enabled": False, "status": "active"},
                {"providerID": "omniroute", "id": "minimax-m3", "enabled": True, "status": "disabled"},
            ]
        }
        self.assertEqual(normalize_models(payload, (MODEL, ALT_MODEL)), ())

    def test_malformed_roster_is_refused(self):
        bad_payloads = [
            {"data": ["not-a-mapping"]},
            {"data": [{"id": "deepseek-v4-pro"}]},  # missing providerID
            {"data": [{"providerID": "omniroute"}]},  # missing id
            {"data": [{"providerID": "  ", "id": "deepseek-v4-pro"}]},  # blank providerID
            {"data": [{"providerID": "omniroute", "id": ""}]},  # blank id
        ]
        for payload in bad_payloads:
            with self.subTest(payload=payload):
                with self.assertRaises(ValidationError):
                    normalize_models(payload, (MODEL,))

    def test_live_beta_18286_triple_normalizes_in_allowlist_order(self):
        allowed = ("omniroute/deepseek-v4-pro", "omniroute/gpt-5.6-luna", "omniroute/MiniMaxM3")
        payload = {
            "data": [
                {"providerID": "opencode", "id": "grok-4-fast", "enabled": True, "status": "active"},
                {"providerID": "omniroute", "id": "gpt-5.6-luna", "enabled": True, "status": "active", "name": "GPT 5.6 Luna"},
                {"providerID": "omniroute", "id": "deepseek-v4-pro", "enabled": True, "status": "active", "name": "DeepSeek V4 Pro"},
                {"providerID": "omniroute", "id": "MiniMaxM3", "enabled": True, "status": "active", "name": "MiniMax M3"},
            ],
            "location": {},
        }
        models = normalize_models(payload, allowed)
        self.assertEqual([item.id for item in models], list(allowed))


class FakeSink:
    def __init__(self):
        self.messages = []
        self.sessions = []
        self.events = []

    def message(self, message):
        self.messages.append(message)

    def session(self, runtime_session_id):
        self.sessions.append(runtime_session_id)

    def event(self, kind, data):
        self.events.append((kind, dict(data)))


class AdapterCase(unittest.TestCase):
    def setUp(self):
        self._auth = mock.patch.dict(os.environ, {PASSWORD_ENV: "fixture-password"})
        self._auth.start()
        self.addCleanup(self._auth.stop)
        self._temp = tempfile.TemporaryDirectory()
        self.addCleanup(self._temp.cleanup)
        self.root = Path(self._temp.name).resolve()
        self.home = self.root / "home"
        self.home.mkdir()
        self.workdir = self.root / "work"
        self.workdir.mkdir()
        self.read_root = self.root / "read"
        self.read_root.mkdir()
        self.request_root = self.root / "also-read"
        self.request_root.mkdir()
        self.agent_dir = self.root / "agent"
        self.agent_dir.mkdir()
        self.binary = self.root / "opencode2"
        self.binary.write_text("#!/bin/sh\n", encoding="utf-8")
        self.mcp_command = self.root / "docs-mcp"
        self.mcp_command.write_text("#!/bin/sh\n", encoding="utf-8")
        self.mcp_servers = {
            "docs": McpConfig("stdio", self.mcp_command, ("--root", "/docs"), ("DOCS_TOKEN",)),
            "unused": McpConfig("stdio", self.mcp_command),
        }
        self.environment = {"DOCS_TOKEN": "secret", "PATH": "/tmp/shim"}
        self.config = runtime_config(
            self.binary,
            self.home,
            models=(MODEL, ALT_MODEL),
            skills=("review",),
            mcp=("docs",),
        )
        self.adapter = OpenCodeAdapter()
        self.profile = AgentProfile("implement", "profile body", False, (self.read_root,))
        self.request = StartRequest(
            runtime=RUNTIME_NAME,
            model=MODEL,
            profile="implement",
            task="do the thing",
            workdir=self.workdir,
            read_roots=(self.request_root,),
        )

    def materialize(self, config=None, mcp_servers=None):
        return self.adapter.materialize(
            config or self.config,
            self.home,
            mcp_servers=self.mcp_servers if mcp_servers is None else mcp_servers,
            inherited_environment=self.environment,
        )

    def prove_service(self, *, pid=None, digest=None):
        digest = self.materialize() if digest is None else digest
        plan = build_service_plan(
            self.config, self.home, port=PORT, inherited_environment=self.environment
        )
        config_home, data_home = service_home_paths(self.home)
        descriptor = verify_isolation(
            plan,
            {
                "healthy": True,
                "pid": os.getpid() if pid is None else pid,
                "version": "2.1.0",
            },
            pid=os.getpid() if pid is None else pid,
            config_hash=digest,
        )
        write_service_descriptor(self.home, descriptor)
        return descriptor

    def prepare(self, request=None, profile=None, mcp_servers=None):
        return self.adapter.prepare(
            request or self.request,
            profile or self.profile,
            self.config,
            self.home,
            self.agent_dir,
            mcp_servers=self.mcp_servers if mcp_servers is None else mcp_servers,
            inherited_environment=self.environment,
        )


class DescribeValidateTests(AdapterCase):
    def test_describe_reports_the_frozen_api_and_no_write(self):
        info = self.adapter.describe()
        self.assertEqual((info.name, info.adapter_api_version), (RUNTIME_NAME, ADAPTER_API_VERSION))
        self.assertIn(Capability.STEER, info.capabilities)
        self.assertNotIn(Capability.WRITE, info.capabilities)
        self.assertNotIn(Capability.EFFORT, info.capabilities)
        self.assertNotIn(Capability.HOOKS, info.capabilities)

    def test_validate_refuses_cli_mode_hooks_and_uncanonical_models(self):
        with self.assertRaises(ValidationError):
            self.adapter.validate(runtime_config(self.binary, self.home, service_mode=None))
        with self.assertRaises(ValidationError):
            self.adapter.validate(
                runtime_config(
                    self.binary,
                    self.home,
                    hooks=(RuntimeHookConfig("PostToolUse", ("agent-run",)),),
                )
            )
        with self.assertRaises(ValidationError):
            self.adapter.validate(runtime_config(self.binary, self.home, models=("minimax-m3",)))


class MaterializeTests(AdapterCase):
    def test_generated_config_is_the_exact_proven_v2_document(self):
        digest = self.materialize()
        path = self.home / CONFIG_RELATIVE_PATH
        text = path.read_text(encoding="utf-8")
        document = json.loads(text)
        self.assertEqual(digest, content_hash(text))
        self.assertEqual(path.stat().st_mode & 0o777, 0o600)
        self.assertEqual(document["model"], MODEL)
        self.assertEqual(document["default_agent"], PRIMARY_AGENT)
        provider = document["providers"]["omniroute"]
        self.assertEqual(provider["env"], ["OMNIROUTE_API_KEY"])
        self.assertEqual(provider["package"], "@opencode-ai/ai/providers/openai-compatible")
        self.assertEqual(provider["settings"]["baseURL"], "http://127.0.0.1:20128/v1")
        self.assertEqual(provider["models"]["deepseek-v4-pro"]["modelID"], "opencode/deepseek-v4-pro")
        self.assertEqual(document["mcp"]["servers"]["docs"]["disabled"], False)
        self.assertNotIn("enabled", document["mcp"]["servers"]["docs"])
        self.assertEqual(digest, self.materialize())


    def test_permission_order_is_preserved_on_disk(self):
        self.materialize()
        text = (self.home / CONFIG_RELATIVE_PATH).read_text(encoding="utf-8")
        permissions = json.loads(text)["agents"][PRIMARY_AGENT]["permissions"]
        self.assertEqual([item["action"] for item in permissions], ["bash", "edit", "write", "webfetch", "external_directory"])
        self.assertLess(text.index('"bash"'), text.index('"external_directory"'))

    def test_mcp_servers_is_a_required_keyword(self):
        with self.assertRaises(TypeError):
            self.adapter.materialize(self.config, self.home)

    def test_only_selected_servers_are_written(self):
        self.materialize()
        document = json.loads((self.home / CONFIG_RELATIVE_PATH).read_text(encoding="utf-8"))
        self.assertEqual(list(document["mcp"]["servers"]), ["docs"])

    def test_unresolved_non_stdio_and_unset_mcp_definitions_are_refused(self):
        with self.assertRaises(ValidationError) as missing:
            self.materialize(mcp_servers={})
        self.assertIn("not in the resolved", str(missing.exception))
        with self.assertRaises(ValidationError) as transport:
            self.materialize(
                mcp_servers={"docs": McpConfig("http", self.mcp_command, (), ())}
            )
        self.assertIn("stdio", str(transport.exception))
        with self.assertRaises(ValidationError):
            self.adapter.materialize(
                self.config,
                self.home,
                mcp_servers=self.mcp_servers,
                inherited_environment={"PATH": "/usr/bin"},
            )
        with self.assertRaises(ValidationError):
            self.materialize(mcp_servers={"docs": {"transport": "stdio"}})

    def test_render_is_pure_and_writes_nothing(self):
        before = sorted(path.name for path in self.home.iterdir())
        render_config(self.config, self.mcp_servers, inherited_environment=self.environment)
        self.assertEqual(sorted(path.name for path in self.home.iterdir()), before)


class ProbeAndPrepareTests(AdapterCase):
    def test_probe_stays_unavailable_until_isolation_is_proven(self):
        health = self.adapter.probe(self.config, self.home)
        self.assertFalse(health.available)
        self.assertIn("unproven", health.reason)
        self.prove_service()
        proven = self.adapter.probe(self.config, self.home)
        self.assertTrue(proven.available)
        self.assertEqual(proven.version, "2.1.0")

    def test_probe_refuses_a_proven_service_without_password(self):
        self.prove_service()
        with mock.patch.dict(os.environ, {}, clear=True):
            health = self.adapter.probe(self.config, self.home)
        self.assertFalse(health.available)
        self.assertEqual(
            health.reason,
            f"{PASSWORD_ENV} must be set to a nonblank value",
        )

    def test_probe_reproves_the_pid_and_the_config_hash(self):
        self.prove_service(pid=2 ** 31 - 1)
        dead = self.adapter.probe(self.config, self.home)
        self.assertFalse(dead.available)
        self.assertIn("is gone", dead.reason)

        self.prove_service()
        self.assertTrue(self.adapter.probe(self.config, self.home).available)
        self.adapter.materialize(
            self.config,
            self.home,
            mcp_servers={"docs": McpConfig("stdio", self.mcp_command, ("--root", "/other"), ())},
            inherited_environment=self.environment,
        )
        changed = self.adapter.probe(self.config, self.home)
        self.assertFalse(changed.available)
        self.assertIn("changed after the service was proven", changed.reason)

    def test_models_and_prepare_refuse_without_a_proven_service(self):
        self.assertEqual(self.adapter.models(self.config, self.home), ())
        with self.assertRaises(ServiceIsolationError):
            self.prepare()

    def test_prepare_attaches_to_the_proven_service_and_starts_no_second_serve(self):
        descriptor = self.prove_service()
        plan = self.prepare()
        config_home, _ = service_home_paths(self.home)
        self.assertEqual(plan.argv, ())
        self.assertEqual(plan.cwd, self.workdir)
        self.assertEqual(plan.environment["XDG_CONFIG_HOME"], str(config_home))
        self.assertEqual(plan.environment["OPENCODE_DISABLE_CLAUDE_CODE"], "1")
        self.assertEqual(plan.environment["PATH"], SERVICE_PATH)
        self.assertNotIn("DOCS_TOKEN", plan.environment)
        self.assertEqual(plan.runtime_stream_path, self.agent_dir / "runtime.jsonl")
        self.assertIn("do the thing", plan.initial_input)
        self.assertEqual(plan.adapter_state["service"]["pid"], descriptor.pid)
        self.assertEqual(plan.adapter_state["service"]["config_hash"], descriptor.config_hash)
        self.assertEqual(
            plan.adapter_state["model"], {"providerID": "omniroute", "modelID": "deepseek-v4-pro"}
        )
        self.assertEqual(plan.adapter_state["agent"], PRIMARY_AGENT)
        self.assertNotIn("write_roots", plan.adapter_state)

    def test_prepare_unions_the_profile_and_request_read_roots(self):
        self.prove_service()
        plan = self.prepare()
        self.assertEqual(
            sorted(plan.adapter_state["read_roots"]),
            sorted([str(self.read_root), str(self.request_root)]),
        )

    def test_prepare_collapses_a_nested_request_root_into_the_profile_root(self):
        self.prove_service()
        nested = self.read_root / "inner"
        nested.mkdir()
        request = _replace(self.request, read_roots=(nested,))
        plan = self.prepare(request=request)
        self.assertEqual(plan.adapter_state["read_roots"], [str(self.read_root)])

    def test_prepare_refuses_a_config_the_service_was_not_proven_with(self):
        self.prove_service()
        with self.assertRaises(ServiceIsolationError) as caught:
            self.prepare(
                mcp_servers={"docs": McpConfig("stdio", self.mcp_command, ("--root", "/other"), ())}
            )
        self.assertIn("proven with a different generated config", str(caught.exception))

    def test_prepare_refuses_write(self):
        self.prove_service()
        writable = AgentProfile("implement", "body", True, (self.read_root,))
        with self.assertRaises(ValidationError) as caught:
            self.prepare(request=_replace(self.request, write=True), profile=writable)
        self.assertIn("no write capability", str(caught.exception))

    def test_prepare_refuses_unsupported_requests(self):
        self.prove_service()
        with self.assertRaises(ValidationError):
            self.prepare(request=_replace(self.request, model="opencode/unlisted"))
        with self.assertRaises(ValidationError):
            self.prepare(request=_replace(self.request, effort="high"))
        with self.assertRaises(ValidationError):
            self.prepare(
                request=_replace(self.request, output_schema={"type": "object", "raw": True})
            )
        with self.assertRaises(ValidationError):
            self.prepare(request=_replace(self.request, output_schema={"type": "string"}))

    def test_prepare_requires_the_mcp_servers_keyword(self):
        self.prove_service()
        with self.assertRaises(TypeError):
            self.adapter.prepare(
                self.request, self.profile, self.config, self.home, self.agent_dir
            )


class FakeService:
    """Enough of the proven v2 service to drive one session end to end."""

    def __init__(self, directory, statuses, message_pages, permission_pages=()):
        self.directory = Path(directory)
        self.statuses = list(statuses)
        self.message_pages = list(message_pages)
        self.permission_pages = list(permission_pages)
        self.calls = []

    def _capture(self, payload):
        body = json.dumps(payload).encode("utf-8")
        descriptor, name = tempfile.mkstemp(dir=self.directory, prefix="reply.", suffix=".json")
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(body)
        return HttpResponse(status=200, path="/fake", body_path=Path(name), body_bytes=len(body))

    def _next(self, pages, default):
        if not pages:
            return default
        return pages.pop(0) if len(pages) > 1 else pages[0]

    def session_status(self):
        self.calls.append(("session_status",))
        entry = self._next(self.statuses, None)
        return {} if entry is None else {"ses_1": entry}

    def messages(self, session_id):
        self.calls.append(("messages", session_id))
        return self._capture(self._next(self.message_pages, []))

    def permissions(self, session_id):
        self.calls.append(("permissions", session_id))
        return self._capture({"data": self._next(self.permission_pages, [])})

    def answer_permission(self, session_id, permission_id, payload):
        self.calls.append(("answer_permission", permission_id, dict(payload)))
        return {}

    def create_session(self, payload):
        self.calls.append(("create_session", dict(payload)))
        return {"id": "ses_1"}

    def prompt_async(self, session_id, payload):
        self.calls.append(("prompt_async", session_id, dict(payload)))
        return {}

    def abort(self, session_id):
        self.calls.append(("abort", session_id))
        return {}


class SessionTests(AdapterCase):
    def setUp(self):
        super().setUp()
        self.captures = self.root / "captures"
        self.captures.mkdir()
        self.clock = 0.0
        self.slept = []

    def advance(self, seconds):
        self.slept.append(seconds)
        self.clock += seconds

    def session(self, service, broker=None, **kwargs):
        self.sink = FakeSink()
        return OpenCodeRuntimeSession(
            service,
            "ses_1",
            self.sink,
            broker=broker if broker is not None else PermissionBroker((self.read_root,)),
            pid=4242,
            response_dir=self.agent_dir,
            model={"providerID": "omniroute", "modelID": "deepseek-v4-pro"},
            sleep=self.advance,
            monotonic=lambda: self.clock,
            **kwargs,
        )

    def remaining(self):
        return sorted(path.name for path in self.captures.iterdir())

    def test_initial_idle_is_ignored_until_the_session_is_busy(self):
        answer = "готово ✅"
        service = FakeService(
            self.captures,
            ["idle", "busy", "retrying", "idle"],
            # One page per fetch: the first idle finds nothing, the last one the answer.
            [[], [message("user", "task"), message("assistant", answer, at=2.0)]],
        )
        session = self.session(service)
        outcome = session.wait(30.0)
        self.assertEqual(outcome.status, AgentStatus.SUCCEEDED)
        self.assertEqual(outcome.runtime_session_id, "ses_1")
        self.assertEqual(session.pid, 4242)
        self.assertEqual(self.sink.sessions, ["ses_1"])
        self.assertEqual([item.content for item in self.sink.messages], ["task", answer])
        # The first idle was probed for output, found none, and kept waiting.
        self.assertEqual([call for call in service.calls if call[0] == "messages"].__len__(), 2)
        self.assertEqual(len(self.remaining()), 1)
        self.assertEqual(self.sink.messages[0].raw_ref, str(self.captures / self.remaining()[0]))

    def test_answer_is_recorded_as_exact_utf8_bytes_and_hash(self):
        from agent_run.verify import DEFAULT_SENTINEL

        answer = "готово ✅"
        service = FakeService(
            self.captures, ["busy", "idle"], [[message("assistant", answer, at=2.0)]]
        )
        outcome = self.session(service).wait(30.0)
        path = self.agent_dir / ANSWER_NAME
        expected = f"{answer}\n{DEFAULT_SENTINEL}\n".encode("utf-8")
        self.assertEqual(path.read_bytes(), expected)
        self.assertEqual(outcome.answer_path, path)
        self.assertEqual(outcome.answer_bytes, len(expected))
        self.assertEqual(outcome.answer_sha256, hashlib.sha256(expected).hexdigest())
        self.assertNotEqual(outcome.answer_bytes, len(answer))

    def test_idle_with_primary_output_settles_immediately(self):
        service = FakeService(self.captures, ["idle"], [[message("assistant", "done", at=1.0)]])
        outcome = self.session(service).wait(30.0)
        self.assertEqual(outcome.status, AgentStatus.SUCCEEDED)
        self.assertEqual(self.slept, [])

    def test_sub_agent_output_alone_does_not_settle_an_idle_session(self):
        service = FakeService(
            self.captures,
            ["idle"],
            [[message("assistant", "verifier notes", agent=VERIFY_AGENT, at=1.0)]],
        )
        session = self.session(service)
        self.assertIsNone(session.wait(0.5))
        self.assertEqual(self.remaining(), [])

    def test_wait_times_out_without_leaking_captures(self):
        service = FakeService(self.captures, ["idle"], [[]])
        session = self.session(service)
        self.assertIsNone(session.wait(1.0))
        self.assertEqual(self.remaining(), [])
        self.assertGreater(len(self.slept), 0)

    def test_error_state_reports_a_failure_and_still_emits_the_transcript(self):
        service = FakeService(
            self.captures,
            ["busy", {"state": "error", "error": {"name": "ProviderError", "message": "429"}}],
            [[message("assistant", "partial", at=2.0)]],
        )
        outcome = self.session(service).wait(30.0)
        self.assertEqual(outcome.status, AgentStatus.FAILED)
        self.assertEqual(outcome.failure_kind, "ProviderError")
        self.assertEqual([item.content for item in self.sink.messages], ["partial"])

    def test_permissions_of_this_session_are_answered_exactly_once(self):
        pending = [
            {"id": "p1", "type": "external_directory", "path": str(self.read_root), "sessionID": "ses_1"},
            {"id": "p2", "type": "bash", "sessionID": "ses_1"},
            {"id": "p3", "type": "external_directory", "path": "/etc", "sessionID": "other"},
        ]
        service = FakeService(
            self.captures, ["busy", "idle"], [[message("assistant", "done", at=1.0)]], [pending, pending]
        )
        outcome = self.session(service).wait(30.0)
        answered = [call for call in service.calls if call[0] == "answer_permission"]
        self.assertEqual(
            [(call[1], call[2]["reply"]) for call in answered],
            [("p1", "once"), ("p2", "reject")],
        )
        self.assertEqual(self.sink.events, [("permissions_blocked", {"bash": 1})])
        self.assertEqual(outcome.status, AgentStatus.SUCCEEDED)

    def test_steer_and_cancel_use_engine_native_calls(self):
        service = FakeService(self.captures, ["busy", "aborted"], [[]])
        session = self.session(service)
        session.steer("focus on tests")
        session.cancel(2.0)
        steered = [call for call in service.calls if call[0] == "prompt_async"][0]
        self.assertEqual(steered[2]["parts"], [{"type": "text", "text": "focus on tests"}])
        self.assertEqual(steered[2]["model"], {"providerID": "omniroute", "modelID": "deepseek-v4-pro"})
        self.assertIn(("abort", "ses_1"), service.calls)
        self.assertEqual(session.wait(30.0).status, AgentStatus.CANCELLED)
        with self.assertRaises(ValidationError):
            session.steer("  ")

    def test_malformed_status_payloads_are_refused(self):
        service = FakeService(self.captures, [["not", "a", "status"]], [[]])
        with self.assertRaises(ValidationError):
            self.session(service).wait(30.0)

    def test_launch_opens_a_session_and_prompts_it_asynchronously(self):
        self.prove_service()
        plan = self.prepare()
        service = FakeService(self.captures, ["busy"], [[]])
        sink = FakeSink()
        session = self.adapter.launch(plan, sink, client=service)
        self.assertEqual(sink.sessions, ["ses_1"])
        self.assertEqual(service.calls[0][0], "create_session")
        self.assertEqual(service.calls[1][0], "prompt_async")
        self.assertEqual(
            service.calls[1][2]["model"], {"providerID": "omniroute", "modelID": "deepseek-v4-pro"}
        )
        self.assertEqual(service.calls[1][2]["agent"], PRIMARY_AGENT)
        self.assertEqual(session.pid, plan.adapter_state["service"]["pid"])


class ProductionSizeTests(unittest.TestCase):
    def test_opencode_production_files_stay_below_hard_gate(self) -> None:
        root = Path(__file__).parents[1] / "src" / "agent_run" / "adapters" / "opencode"
        for path in root.glob("*.py"):
            with self.subTest(path=path.name):
                self.assertLessEqual(len(path.read_text().splitlines()), 700)


def _replace(request, **changes):
    values = {
        "runtime": request.runtime,
        "model": request.model,
        "profile": request.profile,
        "task": request.task,
        "workdir": request.workdir,
        "write": request.write,
        "effort": request.effort,
        "timeout_seconds": request.timeout_seconds,
        "read_roots": request.read_roots,
        "output_schema": request.output_schema,
    }
    values.update(changes)
    return StartRequest(**values)


if __name__ == "__main__":
    unittest.main()
