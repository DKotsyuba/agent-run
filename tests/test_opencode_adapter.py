import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from agent_run.adapters.base import ADAPTER_API_VERSION, Capability
from agent_run.adapters.opencode.adapter import (
    CONFIG_RELATIVE_PATH,
    PRIMARY_AGENT,
    RUNTIME_NAME,
    VERIFY_AGENT,
    OpenCodeAdapter,
    OpenCodeRuntimeSession,
    PermissionBroker,
    extract_answer,
    is_settled,
    normalize_models,
    normalize_outcome,
    normalize_transcript,
)
from agent_run.adapters.opencode.service import (
    SERVICE_HOST,
    ServiceIsolationError,
    service_home_paths,
    verify_isolation,
    write_service_descriptor,
)
from agent_run.config import RuntimeConfig, RuntimeHookConfig
from agent_run.domain import AgentStatus, MessageRole, StartRequest
from agent_run.errors import ValidationError
from agent_run.profiles import AgentProfile

from test_opencode_service import runtime_config


PORT = 41777


def message(role, text, *, agent=PRIMARY_AGENT, synthetic=False, at=1.0):
    return {
        "role": role,
        "agent": agent,
        "synthetic": synthetic,
        "time": {"created": at},
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
        self.assertEqual(self.broker.reply(first)["response"], "once")
        second = self.broker.decide(
            {"id": "p2", "type": "external_directory", "path": str(self.allowed)}
        )
        self.assertFalse(second.granted)
        self.assertIn("already granted once", second.reason)

    def test_directory_outside_read_roots_is_rejected(self):
        decision = self.broker.decide(
            {"id": "p1", "type": "external_directory", "path": str(self.root / "other")}
        )
        self.assertFalse(decision.granted)
        self.assertIsNone(self.broker.granted_directory)
        self.assertEqual(self.broker.reply(decision)["response"], "reject")

    def test_every_other_permission_is_auto_rejected(self):
        for kind in ("bash", "edit", "webfetch", None):
            decision = self.broker.decide({"id": f"p-{kind}", "type": kind, "path": "/"})
            self.assertFalse(decision.granted)
        self.assertEqual(
            dict(self.broker.blocked_summary()),
            {"None": 1, "bash": 1, "edit": 1, "webfetch": 1},
        )

    def test_unusable_permission_payloads_are_refused(self):
        with self.assertRaises(ValidationError):
            self.broker.decide({"type": "external_directory", "path": str(self.allowed)})
        relative = self.broker.decide({"id": "p1", "type": "external_directory", "path": "allowed"})
        self.assertFalse(relative.granted)


class TranscriptTests(unittest.TestCase):
    def test_all_assistant_text_after_the_last_real_user_is_preserved(self):
        payload = {
            "messages": [
                message("user", "first task"),
                message("assistant", "old answer"),
                message("user", "real task", at=2.0),
                message("assistant", "attempt one failed", at=3.0),
                message("user", "retrying", synthetic=True, at=4.0),
                message("assistant", "attempt two answer", at=5.0),
            ]
        }
        self.assertEqual(
            extract_answer(payload), "attempt one failed\n\nattempt two answer"
        )

    def test_sub_agent_output_is_not_mistaken_for_the_answer(self):
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
        payload = {
            "messages": [
                message("user", "task"),
                {"role": "assistant", "parts": [{"type": "tool", "tool": "bash"}]},
                message("assistant", "answer", agent=VERIFY_AGENT, at=2.0),
            ]
        }
        messages = normalize_transcript(payload, raw_ref="/tmp/reply.json")
        self.assertEqual([item.role for item in messages], [MessageRole.USER, MessageRole.ASSISTANT])
        self.assertEqual(messages[1].name, VERIFY_AGENT)
        self.assertEqual(messages[1].raw_ref, "/tmp/reply.json")

    def test_unknown_role_is_refused(self):
        with self.assertRaises(ValidationError):
            normalize_transcript({"messages": [message("tool", "x")]})


class OutcomeTests(unittest.TestCase):
    def test_states_normalize_to_terminal_outcomes(self):
        self.assertEqual(normalize_outcome({"state": "completed"}).status, AgentStatus.SUCCEEDED)
        self.assertEqual(normalize_outcome({"state": "aborted"}).status, AgentStatus.CANCELLED)
        self.assertEqual(normalize_outcome({"state": "timeout"}).status, AgentStatus.TIMED_OUT)

    def test_reported_error_wins_over_a_completed_state(self):
        outcome = normalize_outcome(
            {"state": "completed", "error": {"name": "ProviderError", "message": "429"}},
            runtime_session_id="s1",
        )
        self.assertEqual(outcome.status, AgentStatus.FAILED)
        self.assertEqual(outcome.failure_kind, "ProviderError")
        self.assertEqual(outcome.runtime_session_id, "s1")

    def test_active_states_are_not_terminal(self):
        self.assertFalse(is_settled({"state": "running"}))
        self.assertTrue(is_settled({"state": "aborted"}))
        with self.assertRaises(ValidationError):
            normalize_outcome({"state": "running"})


class ModelRosterTests(unittest.TestCase):
    def test_roster_is_intersected_with_the_allowlist_in_config_order(self):
        payload = {
            "providers": [
                {"models": {"a": {"id": "MiniMaxM3", "name": "MiniMax M3"}}},
                {"models": [{"id": "deepseek-v4-pro"}, {"id": "unlisted"}]},
            ]
        }
        models = normalize_models(payload, ("deepseek-v4-pro", "MiniMaxM3", "absent"))
        self.assertEqual([item.id for item in models], ["deepseek-v4-pro", "MiniMaxM3"])
        self.assertEqual(models[1].description, "MiniMax M3")

    def test_malformed_roster_is_refused(self):
        with self.assertRaises(ValidationError):
            normalize_models({"providers": [{"models": [{"name": "no id"}]}]}, ("x",))


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
        self._temp = tempfile.TemporaryDirectory()
        self.addCleanup(self._temp.cleanup)
        self.root = Path(self._temp.name).resolve()
        self.home = self.root / "home"
        self.home.mkdir()
        self.workdir = self.root / "work"
        self.workdir.mkdir()
        self.read_root = self.root / "read"
        self.read_root.mkdir()
        self.agent_dir = self.root / "agent"
        self.agent_dir.mkdir()
        self.binary = self.root / "opencode2"
        self.binary.write_text("#!/bin/sh\n", encoding="utf-8")
        self.config = runtime_config(self.binary, self.home, models=("MiniMaxM3", "deepseek-v4-pro"))
        self.adapter = OpenCodeAdapter()
        self.profile = AgentProfile("implement", "profile body", True, (self.read_root,))
        self.request = StartRequest(
            runtime=RUNTIME_NAME,
            model="MiniMaxM3",
            profile="implement",
            task="do the thing",
            workdir=self.workdir,
            write=True,
        )

    def prove_service(self):
        from agent_run.adapters.opencode.service import build_service_plan

        plan = build_service_plan(self.config, self.home, port=PORT)
        config_home, data_home = service_home_paths(self.home)
        descriptor = verify_isolation(
            plan,
            {
                "config_home": str(config_home),
                "data_home": str(data_home),
                "host": SERVICE_HOST,
                "port": PORT,
                "version": "2.1.0",
            },
        )
        write_service_descriptor(self.home, descriptor)
        return descriptor


class DescribeValidateTests(AdapterCase):
    def test_describe_reports_the_frozen_api_and_no_effort(self):
        info = self.adapter.describe()
        self.assertEqual((info.name, info.adapter_api_version), (RUNTIME_NAME, ADAPTER_API_VERSION))
        self.assertIn(Capability.STEER, info.capabilities)
        self.assertNotIn(Capability.EFFORT, info.capabilities)
        self.assertNotIn(Capability.HOOKS, info.capabilities)

    def test_validate_refuses_cli_mode_and_hooks(self):
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

    def test_materialize_writes_only_below_the_generated_config_home(self):
        digest = self.adapter.materialize(self.config, self.home)
        path = self.home / CONFIG_RELATIVE_PATH
        document = json.loads(path.read_text(encoding="utf-8"))
        self.assertEqual(len(digest), 64)
        self.assertEqual(path.stat().st_mode & 0o777, 0o600)
        self.assertIn(PRIMARY_AGENT, document["agent"])
        self.assertEqual(document["permission"]["external_directory"], "ask")
        self.assertEqual(document["permission"]["bash"], "deny")
        self.assertEqual(digest, self.adapter.materialize(self.config, self.home))


class ProbeAndPrepareTests(AdapterCase):
    def test_probe_stays_unavailable_until_isolation_is_proven(self):
        health = self.adapter.probe(self.config, self.home)
        self.assertFalse(health.available)
        self.assertIn("unproven", health.reason)
        self.prove_service()
        proven = self.adapter.probe(self.config, self.home)
        self.assertTrue(proven.available)
        self.assertEqual(proven.version, "2.1.0")

    def test_models_and_prepare_refuse_without_a_proven_service(self):
        self.assertEqual(self.adapter.models(self.config, self.home), ())
        with self.assertRaises(ServiceIsolationError):
            self.adapter.prepare(
                self.request, self.profile, self.config, self.home, self.agent_dir
            )

    def test_prepare_carries_isolated_environment_and_bounded_state(self):
        self.prove_service()
        plan = self.adapter.prepare(
            self.request, self.profile, self.config, self.home, self.agent_dir
        )
        config_home, _ = service_home_paths(self.home)
        self.assertEqual(plan.cwd, self.workdir)
        self.assertEqual(plan.environment["XDG_CONFIG_HOME"], str(config_home))
        self.assertEqual(plan.environment["OPENCODE_DISABLE_CLAUDE_CODE"], "1")
        self.assertEqual(plan.runtime_stream_path, self.agent_dir / "runtime.jsonl")
        self.assertIn("do the thing", plan.initial_input)
        self.assertEqual(plan.adapter_state["write_roots"], [str(self.workdir)])
        self.assertEqual(plan.adapter_state["read_roots"], [str(self.read_root)])
        self.assertEqual(plan.adapter_state["agent"], PRIMARY_AGENT)

    def test_prepare_refuses_unsupported_requests(self):
        self.prove_service()

        def prepare(request, profile=None):
            return self.adapter.prepare(
                request, profile or self.profile, self.config, self.home, self.agent_dir
            )

        with self.assertRaises(ValidationError):
            prepare(_replace(self.request, model="unlisted-model"))
        with self.assertRaises(ValidationError):
            prepare(_replace(self.request, effort="high"))
        with self.assertRaises(ValidationError):
            prepare(_replace(self.request, output_schema={"type": "object", "raw": True}))
        with self.assertRaises(ValidationError):
            prepare(_replace(self.request, output_schema={"type": "string"}))
        with self.assertRaises(ValidationError):
            prepare(self.request, AgentProfile("review", "body", False, ()))

    def test_read_root_never_becomes_a_write_root(self):
        self.prove_service()
        nested = self.read_root / "inner"
        nested.mkdir()
        profile = AgentProfile("implement", "body", True, (self.read_root,))
        request = _replace(self.request, workdir=nested)
        with self.assertRaises(ValidationError):
            self.adapter.prepare(request, profile, self.config, self.home, self.agent_dir)


class FakeClient:
    def __init__(self, states, messages):
        self.states = list(states)
        self._messages = messages
        self.calls = []

    def poll(self, path, ready, *, deadline_seconds, interval_seconds=0.25):
        while self.states:
            payload = self.states.pop(0)
            if ready(payload):
                return payload
        raise AssertionError("state never settled")

    def messages(self, session_id):
        self.calls.append(("messages", session_id))
        return _Captured(self._messages)

    def create_session(self, payload):
        self.calls.append(("create_session", payload))
        return {"id": "ses_1"}

    def prompt(self, session_id, payload):
        self.calls.append(("prompt", session_id, payload))
        return {"ok": True}

    def steer(self, session_id, payload):
        self.calls.append(("steer", session_id, payload))
        return {"ok": True}

    def interrupt(self, session_id):
        self.calls.append(("interrupt", session_id))
        return {"ok": True}

    def answer_permission(self, session_id, permission_id, payload):
        self.calls.append(("permission", permission_id, dict(payload)))
        return {"ok": True}


class _Captured:
    def __init__(self, payload):
        self._payload = payload

    def mapping(self):
        return self._payload


class SessionTests(AdapterCase):
    def session(self, client, broker=None):
        self.sink = FakeSink()
        return OpenCodeRuntimeSession(client, "ses_1", self.sink, broker=broker)

    def test_wait_emits_transcript_and_blocked_summary(self):
        client = FakeClient(
            [{"state": "running"}, {"state": "completed"}],
            {"messages": [message("user", "task"), message("assistant", "answer", at=2.0)]},
        )
        broker = PermissionBroker((self.read_root,))
        broker.decide({"id": "p1", "type": "bash", "path": "/"})
        session = self.session(client, broker)
        outcome = session.wait(5.0)
        self.assertEqual(outcome.status, AgentStatus.SUCCEEDED)
        self.assertEqual(outcome.runtime_session_id, "ses_1")
        self.assertEqual(self.sink.sessions, ["ses_1"])
        self.assertEqual([item.content for item in self.sink.messages], ["task", "answer"])
        self.assertEqual(self.sink.events, [("permissions_blocked", {"bash": 1})])
        self.assertIsNone(session.pid)

    def test_steer_and_cancel_use_engine_native_calls(self):
        client = FakeClient([{"state": "aborted"}], {"messages": []})
        session = self.session(client)
        session.steer("focus on tests")
        session.cancel(2.0)
        self.assertEqual(client.calls[0][0], "steer")
        self.assertEqual(client.calls[1], ("interrupt", "ses_1"))
        self.assertEqual(session.wait(1.0).status, AgentStatus.CANCELLED)
        with self.assertRaises(ValidationError):
            session.steer("  ")

    def test_permissions_are_answered_once_and_rejected_otherwise(self):
        client = FakeClient([{"state": "completed"}], {"messages": []})
        session = self.session(client, PermissionBroker((self.read_root,)))
        decisions = session.resolve_permissions(
            {
                "permissions": [
                    {"id": "p1", "type": "external_directory", "path": str(self.read_root)},
                    {"id": "p2", "type": "external_directory", "path": str(self.root)},
                    {"id": "p3", "type": "bash", "path": "/"},
                ]
            }
        )
        self.assertEqual([item.granted for item in decisions], [True, False, False])
        self.assertEqual(
            [call[2]["response"] for call in client.calls], ["once", "reject", "reject"]
        )

    def test_launch_opens_a_session_and_prompts_it(self):
        self.prove_service()
        plan = self.adapter.prepare(
            self.request, self.profile, self.config, self.home, self.agent_dir
        )
        client = FakeClient([], {"messages": []})
        sink = FakeSink()
        session = self.adapter.launch(plan, sink, client=client)
        self.assertEqual(sink.sessions, ["ses_1"])
        self.assertEqual(client.calls[0][0], "create_session")
        self.assertEqual(client.calls[1][0], "prompt")
        self.assertEqual(client.calls[1][2]["model"], "MiniMaxM3")
        self.assertIsNone(session.pid)


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
