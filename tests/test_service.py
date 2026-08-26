from __future__ import annotations

import hashlib
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

from agent_run.adapters.base import (
    ADAPTER_API_VERSION,
    Capability,
    LaunchPlan,
    ModelInfo,
    RuntimeHealth,
    RuntimeInfo,
)
from agent_run.config import Config, ProfilesConfig, RuntimeConfig
from agent_run.domain import (
    AgentStatus,
    Message,
    MessageRole,
    OrchestratorRef,
    Outcome,
    StartRequest,
)
from agent_run.errors import StateTransitionError, ValidationError
from agent_run.paths import agent_dir
from agent_run.service import AgentQuery, AgentService
from agent_run.state.store import StateStore


class FakeAdapter:
    def __init__(self) -> None:
        self.reset()

    def reset(self) -> None:
        self.capabilities = frozenset(Capability)
        self.validate_calls = 0
        self.materialize_calls = 0
        self.models_calls = 0
        self.limits_calls = 0
        self.prepare_calls = 0

    def describe(self):
        return RuntimeInfo("fake", ADAPTER_API_VERSION, self.capabilities)

    def validate(self, config):
        self.validate_calls += 1

    def materialize(self, config, home, *, mcp_servers):
        self.materialize_calls += 1
        return "cfg-1"

    def probe(self, config, home):
        return RuntimeHealth(True, "1", True, None)

    def models(self, config, home):
        self.models_calls += 1
        return (ModelInfo("model", "fake model", ("high",)),)

    def limits(self, config, home):
        self.limits_calls += 1
        raise AssertionError("service limits must use stored samples")

    def prepare(self, request, profile, config, home, agent_dir, *, mcp_servers):
        self.prepare_calls += 1
        return LaunchPlan(
            ("fake",), request.workdir, {}, request.task, agent_dir / "runtime.jsonl", {}
        )

    def launch(self, plan, sink):
        raise AssertionError("AgentService uses the injected launch seam")


ADAPTER = FakeAdapter()


class AgentServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        ADAPTER.reset()
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name).resolve()
        self.workdir = self.root / "work"
        self.runtime_home = self.root / "runtime"
        self.profiles = self.root / "profiles"
        for path in (self.workdir, self.runtime_home, self.profiles):
            path.mkdir()
        (self.profiles / "profile.md").write_text(
            "+++\nwrite = true\n+++\nDo the requested work.\n", encoding="utf-8"
        )
        self.config = Config(
            schema_version=1,
            profiles=ProfilesConfig(self.profiles),
            runtimes={
                "fake": RuntimeConfig(
                    True,
                    f"{__name__}:ADAPTER",
                    Path("/bin/true"),
                    self.runtime_home,
                    ("model",),
                )
            },
        )
        self.store = StateStore.initialize(self.root / "state.db")
        self.launched: list[tuple] = []
        self.service = AgentService(
            self.config,
            self.store,
            self.root,
            launch=lambda *args: self.launched.append(args),
            now=lambda: 100.0,
        )

    def tearDown(self) -> None:
        self.store.close()
        self.temporary.cleanup()

    def request(
        self,
        *,
        request_id: str | None = None,
        task: str = "do work",
        write: bool = False,
        model: str = "model",
    ) -> StartRequest:
        return StartRequest(
            "fake",
            model,
            "profile",
            task,
            self.workdir,
            write=write,
            request_id=request_id,
        )

    def start(self, request_id: str, task: str = "do work"):
        return self.service.start(self.request(request_id=request_id, task=task))

    def terminal(self, agent_id, status=AgentStatus.CANCELLED) -> None:
        self.store.transition(agent_id, status, outcome=Outcome(status), at=101)

    def test_complete_refusal_happens_before_agent_row(self) -> None:
        ADAPTER.capabilities = frozenset(
            capability for capability in Capability if capability is not Capability.WRITE
        )
        with self.assertRaisesRegex(ValidationError, "lacks required capabilities"):
            self.service.start(self.request(write=True, request_id="refused-write"))
        self.assertEqual(self.store.list_agents(), [])
        self.assertEqual(self.launched, [])

        ADAPTER.capabilities = frozenset(Capability)
        with self.assertRaisesRegex(ValidationError, "model is not configured"):
            self.service.start(
                self.request(model="missing", request_id="refused-model")
            )
        self.assertEqual(self.store.list_agents(), [])
        self.assertEqual(ADAPTER.materialize_calls, 0)

    def test_request_id_returns_one_agent_and_launches_once(self) -> None:
        first = self.start("same-request", task="  do   work  ")
        second = self.start("same-request", task="  do   work  ")

        self.assertTrue(first.created)
        self.assertFalse(second.created)
        self.assertEqual(first.agent_id, second.agent_id)
        self.assertEqual(len(self.launched), 1)
        self.assertEqual(len(self.store.list_agents()), 1)
        self.assertEqual(first.agent.task_summary, "do work")
        launched = self.launched[0]
        self.assertEqual(launched[0], first.agent_id)
        self.assertIs(launched[2], ADAPTER)
        self.assertIsInstance(launched[3], LaunchPlan)

    def test_service_passes_caps_and_refusal_never_launches_or_creates_artifacts(self) -> None:
        runtime = replace(
            self.config.runtimes["fake"], max_active_agents=1
        )
        config = replace(
            self.config,
            core=replace(self.config.core, max_active_agents=1),
            runtimes={"fake": runtime},
        )
        request = self.request(request_id="already-active")
        existing = self.store.create_agent(
            request,
            task_summary="do work",
            config_revision="cfg-1",
            at=99,
        )
        launched = []
        service = AgentService(
            config,
            self.store,
            self.root,
            launch=lambda *args: launched.append(args),
            now=lambda: 100.0,
        )

        with patch.object(
            self.store,
            "create_agent_limited",
            wraps=self.store.create_agent_limited,
        ) as limited:
            duplicate = service.start(request)
            with self.assertRaisesRegex(
                ValidationError, "global active agent limit reached"
            ):
                service.start(self.request(request_id="distinct-request"))

        self.assertFalse(duplicate.created)
        self.assertEqual(duplicate.agent_id, existing.agent_id)
        self.assertEqual(launched, [])
        self.assertEqual(limited.call_count, 2)
        for call in limited.call_args_list:
            self.assertEqual(call.kwargs["global_limit"], 1)
            self.assertEqual(call.kwargs["runtime_limit"], 1)
        self.assertEqual(len(self.store.list_agents()), 1)
        self.assertEqual(
            self.store.connection.execute(
                "SELECT COUNT(*) FROM events WHERE kind = 'created'"
            ).fetchone()[0],
            1,
        )
        self.assertFalse((self.root / "agents").exists())

    def test_launch_failure_is_durable_and_idempotent_retry_never_relaunches(self) -> None:
        calls: list[tuple] = []

        def fail_launch(*args) -> None:
            calls.append(args)
            raise ValidationError("ready failed")

        service = AgentService(
            self.config,
            self.store,
            self.root,
            launch=fail_launch,
            now=lambda: 100.0,
        )
        request = self.request(request_id="launch-failure")
        with self.assertRaisesRegex(ValidationError, "ready failed"):
            service.start(request)

        row = self.store.list_agents()[0]
        agent_id = row["id"]
        self.assertEqual(row["status"], "failed")
        self.assertEqual(row["failure_kind"], "supervisor_start_failed")
        self.assertEqual(row["failure_text"], "ready failed")
        view = service.get(agent_id)
        self.assertEqual(view.delivery.state, "waiting_binding")
        event = self.store.connection.execute(
            """SELECT kind, data_json FROM events
               WHERE agent_id = ? ORDER BY seq DESC LIMIT 1""",
            (agent_id,),
        ).fetchone()
        self.assertEqual(event["kind"], "supervisor_start_failed")
        self.assertIn(str(agent_id), event["data_json"])

        retry = service.start(request)
        self.assertFalse(retry.created)
        self.assertIs(retry.agent.status, AgentStatus.FAILED)
        self.assertEqual(len(calls), 1)

    def test_list_has_exact_total_and_explicit_offset_completeness(self) -> None:
        for index in range(3):
            self.start(f"request-{index}", task=f"task {index}")

        first = self.service.list(AgentQuery(active=True, limit=2))
        self.assertEqual(first.total, 3)
        self.assertEqual(len(first.items), 2)
        self.assertFalse(first.complete)
        self.assertEqual(first.next_offset, 2)

        second = self.service.list(AgentQuery(active=True, offset=2, limit=2))
        self.assertEqual(second.total, 3)
        self.assertEqual(len(second.items), 1)
        self.assertTrue(second.complete)
        self.assertIsNone(second.next_offset)

    def test_list_and_transcript_share_the_bounded_page_limit(self) -> None:
        with self.assertRaisesRegex(ValidationError, "limit must not exceed 1000"):
            AgentQuery(limit=1001)
        agent_id = self.start("page-cap").agent_id
        with self.assertRaisesRegex(ValidationError, "limit must not exceed 1000"):
            self.service.transcript(agent_id, limit=1001)

    def test_transcript_cursor_is_explicit_and_raw_ref_is_preserved(self) -> None:
        agent_id = self.start("transcript").agent_id
        first = self.store.append_message(
            agent_id, Message(1, MessageRole.USER, "one")
        )
        second = self.store.append_message(
            agent_id,
            Message(2, MessageRole.TOOL_RESULT, "two", raw_ref="raw/two.json"),
        )
        third = self.store.append_message(
            agent_id, Message(3, MessageRole.ASSISTANT, "three")
        )

        page = self.service.transcript(agent_id, limit=2)
        self.assertEqual([item.seq for item in page.messages], [first, second])
        self.assertEqual(page.messages[1].raw_ref, "raw/two.json")
        self.assertFalse(page.complete)
        self.assertEqual(page.next_cursor, second)

        tail = self.service.transcript(agent_id, cursor=second, limit=2)
        self.assertEqual([item.seq for item in tail.messages], [third])
        self.assertTrue(tail.complete)
        self.assertIsNone(tail.next_cursor)

    def test_answer_verifies_path_size_hash_and_bounds_inline_content(self) -> None:
        agent_id = self.start("answer").agent_id
        directory = agent_dir(agent_id, self.root)
        directory.mkdir(parents=True)
        path = directory / "answer.md"
        body = b"sealed answer"
        path.write_bytes(body)
        digest = hashlib.sha256(body).hexdigest()
        self.store.transition(agent_id, AgentStatus.STARTING, at=101)
        self.store.transition(agent_id, AgentStatus.RUNNING, at=102)
        self.store.transition(
            agent_id,
            AgentStatus.SUCCEEDED,
            outcome=Outcome(
                AgentStatus.SUCCEEDED,
                answer_path=path,
                answer_bytes=len(body),
                answer_sha256=digest,
            ),
            at=103,
        )

        answer = self.service.answer(agent_id)
        self.assertEqual(answer.content, body.decode())
        self.assertTrue(answer.inline_complete)
        bounded = AgentService(
            self.config,
            self.store,
            self.root,
            launch=lambda *_: None,
            now=lambda: 100.0,
            max_inline_answer_bytes=4,
        ).answer(agent_id)
        self.assertIsNone(bounded.content)
        self.assertFalse(bounded.inline_complete)

        path.write_bytes(b"sealed answeX")
        with self.assertRaisesRegex(ValidationError, "hash does not match"):
            self.service.answer(agent_id)

    def test_steer_is_capability_gated_before_enqueue_and_errors_stay_typed(self) -> None:
        agent_id = self.start("steer").agent_id
        ADAPTER.capabilities = frozenset(
            capability for capability in Capability if capability is not Capability.STEER
        )
        with self.assertRaises(ValidationError):
            self.service.steer(agent_id, "finish")
        self.assertEqual(
            self.store.connection.execute("SELECT COUNT(*) FROM commands").fetchone()[0],
            0,
        )

        ADAPTER.capabilities = frozenset(Capability)
        command = self.service.steer(agent_id, "finish")
        self.assertEqual((command.kind, command.state), ("steer", "pending"))

        terminal_id = self.start("terminal-command").agent_id
        self.terminal(terminal_id)
        with self.assertRaises(StateTransitionError):
            self.service.cancel(terminal_id)

    def test_binding_summary_models_and_stored_limits_share_the_service(self) -> None:
        agent_id = self.start("binding", task="safe task").agent_id
        ref = OrchestratorRef("codex_queue", "session-1", "turn-1")
        delivery = self.service.bind(agent_id, ref)
        self.assertTrue(delivery.bound)
        self.assertEqual(delivery.state, "not_created")
        with self.assertRaisesRegex(ValidationError, "immutable"):
            self.service.bind(
                agent_id, OrchestratorRef("codex_queue", "other-session")
            )

        self.terminal(agent_id)
        view = self.service.get(agent_id)
        self.assertEqual(view.delivery.state, "pending")
        self.assertEqual(self.service.summary(agent_id=agent_id).agents, (view,))
        self.assertEqual(self.service.summary(orchestrator=ref).total, 0)
        with self.assertRaises(ValidationError):
            self.service.summary()
        with self.assertRaises(ValidationError):
            self.service.summary(agent_id=agent_id, orchestrator=ref)

        self.assertEqual(tuple(self.service.models()), ("fake",))
        self.assertEqual(self.service.models()["fake"][0].id, "model")
        self.store.insert_capacity_sample(
            runtime="fake",
            lane="main",
            window="5h",
            source="test",
            payload={},
            remaining_percent=50,
            reset_at=200,
            observed_at=100,
            valid_until=150,
        )
        limits = self.service.limits()
        self.assertEqual(len(limits.items), 1)
        self.assertEqual(limits.items[0].key.runtime, "fake")
        self.assertEqual(ADAPTER.limits_calls, 0)

    def test_from_home_is_the_single_composition_root(self) -> None:
        (self.root / "config.toml").write_text(
            f"""schema_version = 1

[profiles]
directory = "{self.profiles}"

[runtimes.fake]
enabled = true
adapter = "{__name__}:ADAPTER"
binary = "/bin/true"
home = "{self.runtime_home}"
models = ["model"]
""",
            encoding="utf-8",
        )
        launched: list[tuple] = []
        service = AgentService.from_home(
            self.root,
            launch=lambda *args: launched.append(args),
            now=lambda: 100.0,
        )
        try:
            result = service.start(self.request(request_id="composed"))
            self.assertTrue(result.created)
            self.assertEqual(len(launched), 1)
        finally:
            service.close()


if __name__ == "__main__":
    unittest.main()
