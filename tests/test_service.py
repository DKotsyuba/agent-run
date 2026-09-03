from __future__ import annotations

import hashlib
import tempfile
import threading
import time
import unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

from agent_run.accounts import account_auth_source, account_runtime_home
from agent_run.adapters.base import (
    ADAPTER_API_VERSION,
    Capability,
    LaunchPlan,
    ModelInfo,
    RuntimeHealth,
    RuntimeInfo,
)
from agent_run.config import Config, ProfilesConfig, RuntimeAuthConfig, RuntimeConfig
from agent_run.domain import (
    AgentStatus,
    Message,
    MessageRole,
    OrchestratorRef,
    Outcome,
    StartRequest,
)
from agent_run.errors import StateTransitionError, ValidationError
from agent_run.delivery.base import DeliveryAttemptEvidence
from agent_run.launch_evidence import FAILURE_KIND_BOOTSTRAP, SupervisorBootstrapError
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
        self.materialize_configs = []
        self.materialize_homes = []
        self.skills_roots = []
        self.models_calls = 0
        self.models_result: tuple[ModelInfo, ...] = (
            ModelInfo("model", "fake model", ("high",)),
        )
        self.probe_health = RuntimeHealth(True, "1", True, None)
        self.limits_calls = 0
        self.prepare_calls = 0
        self.prepare_dirs = []
        self.prepare_profiles = []
        self.prepare_error = None

    def describe(self):
        return RuntimeInfo("fake", ADAPTER_API_VERSION, self.capabilities)

    def validate(self, config):
        self.validate_calls += 1

    def materialize(self, config, home, *, mcp_servers, skills_root):
        self.materialize_calls += 1
        self.materialize_configs.append(config)
        self.materialize_homes.append(home)
        self.skills_roots.append(skills_root)
        return "cfg-1"

    def probe(self, config, home):
        return self.probe_health

    def models(self, config, home):
        self.models_calls += 1
        return self.models_result

    def limits(self, config, home):
        self.limits_calls += 1
        raise AssertionError("service limits must use stored samples")

    def prepare(self, request, profile, config, home, agent_dir, *, mcp_servers):
        self.prepare_calls += 1
        self.prepare_dirs.append(agent_dir)
        self.prepare_profiles.append(profile)
        if self.prepare_error is not None:
            raise self.prepare_error
        return LaunchPlan(
            ("fake",), request.workdir, {}, request.task, agent_dir / "runtime.jsonl", {},
            agent_dir / "answer.md",
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
        self.service.close()
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
        """Start through the async service and wait for the fake launch."""

        launched_before = len(self.launched)
        result = self.service.start(self.request(request_id=request_id, task=task))
        if result.created:
            self.wait_until(lambda: len(self.launched) > launched_before)
        return result

    def wait_until(self, predicate, *, timeout: float = 2.0) -> None:
        """Wait boundedly for an async test predicate or fail the test."""

        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if predicate():
                return
            time.sleep(0.01)
        self.fail("asynchronous condition did not become true")

    def terminal(self, agent_id, status=AgentStatus.CANCELLED) -> None:
        self.store.transition(agent_id, status, outcome=Outcome(status), at=101)

    def test_account_resolution_uses_sibling_home_and_store_auth(self) -> None:
        runtime = self.config.runtimes["fake"]
        auth_source = self.root / "accounts" / "fake" / "personal2" / "auth.json"
        auth_source.parent.mkdir(parents=True)
        auth_source.write_text("{}", encoding="utf-8")
        self.service = AgentService(
            replace(self.config, runtimes={"fake": replace(
                runtime,
                accounts=("personal2",),
                default_account="personal2",
                auth=RuntimeAuthConfig("file_link", self.root / "auth", "auth.json"),
            )}),
            self.store, self.root, launch=lambda *args: self.launched.append(args), now=lambda: 100.0,
        )
        self.service.start(self.request(request_id="account"))
        self.wait_until(lambda: bool(ADAPTER.materialize_homes))
        self.assertEqual(ADAPTER.materialize_homes[-1], self.runtime_home.with_name("runtime@personal2"))
        self.assertEqual(ADAPTER.materialize_configs[-1].auth.source, auth_source)
        self.assertEqual((self.runtime_home.with_name("runtime@personal2")).is_dir(), True)

    def test_start_hands_the_adapter_a_profile_carrying_its_role_assignment(self) -> None:
        """The profile is where agent-run assigns the shared role contract.

        The adapters only inject ``profile.body``, so if this wiring were
        dropped every runtime would silently stop assigning roles.
        """

        runtime = self.config.runtimes["fake"]
        service = AgentService(
            replace(
                self.config,
                runtimes={"fake": replace(runtime, skills=("role-profile",))},
            ),
            self.store,
            self.root,
            launch=lambda *args: self.launched.append(args),
            now=lambda: 100.0,
        )
        service.start(self.request(request_id="assigned"))
        self.wait_until(lambda: bool(ADAPTER.prepare_profiles))
        body = ADAPTER.prepare_profiles[-1].body
        self.assertTrue(body.startswith("Do the requested work."))
        self.assertIn("Your assigned role is role-profile.", body)

        # No matching skill shipped: the same start leaves the body alone.
        self.start("unassigned")
        self.assertEqual(ADAPTER.prepare_profiles[-1].body, "Do the requested work.")
        service.close()

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

    def test_start_returns_before_prepare_and_pending_replay_stays_single(self) -> None:
        """Slow prepare must not block admission, status, or idempotent replay."""

        entered = threading.Event()
        release = threading.Event()
        original = ADAPTER.prepare

        def blocked_prepare(*args, **kwargs):
            """Hold prepare until the test has exercised the admission path."""

            entered.set()
            release.wait(2)
            return original(*args, **kwargs)

        with patch.object(ADAPTER, "prepare", side_effect=blocked_prepare):
            started = time.monotonic()
            first = self.service.start(self.request(request_id="pending-replay"))
            self.assertLess(time.monotonic() - started, 0.5)
            self.assertIs(first.agent.status, AgentStatus.STARTING)
            self.assertTrue(entered.wait(1))
            self.assertIs(self.service.get(first.agent_id).status, AgentStatus.STARTING)

            replay = self.service.start(self.request(request_id="pending-replay"))
            self.assertFalse(replay.created)
            self.assertEqual(replay.agent_id, first.agent_id)
            sibling_started = time.monotonic()
            sibling = self.service.start(self.request(request_id="pending-sibling"))
            self.assertLess(time.monotonic() - sibling_started, 0.5)
            self.assertIs(sibling.agent.status, AgentStatus.STARTING)
            with self.assertRaisesRegex(
                ValidationError, "request_id was reused for a different request"
            ):
                self.service.start(
                    self.request(request_id="pending-replay", task="changed")
                )
            release.set()
            self.wait_until(lambda: len(self.launched) == 2)

    def test_cancel_during_prepare_prevents_launch(self) -> None:
        """Durable cancellation during prepare must stop before spawn."""

        entered = threading.Event()
        release = threading.Event()
        original = ADAPTER.prepare

        def blocked_prepare(*args, **kwargs):
            """Expose the cancellation window inside adapter preparation."""

            entered.set()
            release.wait(2)
            return original(*args, **kwargs)

        with patch.object(ADAPTER, "prepare", side_effect=blocked_prepare):
            accepted = self.service.start(self.request(request_id="cancel-prepare"))
            self.assertTrue(entered.wait(1))
            self.service.cancel(accepted.agent_id)
            release.set()
            self.wait_until(
                lambda: self.store.get_agent(accepted.agent_id)["status"]
                == AgentStatus.CANCELLED.value
            )
        self.assertEqual(self.launched, [])

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

    def test_default_timeout_is_resolved_once_and_explicit_value_is_preserved(self) -> None:
        """Resolve default and explicit timeout values independently of launch order."""
        config = replace(
            self.config,
            core=replace(self.config.core, default_timeout_seconds=7),
        )
        launched = []
        service = AgentService(
            config,
            self.store,
            self.root,
            launch=lambda *args: launched.append(args),
            now=lambda: 100.0,
        )

        defaulted = service.start(self.request(request_id="default-timeout"))
        explicit = service.start(
            replace(
                self.request(request_id="explicit-timeout"),
                timeout_seconds=480,
            )
        )

        self.wait_until(lambda: len(launched) == 2)
        timeouts = {args[1].request_id: args[1].timeout_seconds for args in launched}
        self.assertEqual(timeouts, {"default-timeout": 7, "explicit-timeout": 480})
        self.assertEqual(self.store.get_agent(defaulted.agent_id)["timeout_seconds"], 7)
        self.assertEqual(self.store.get_agent(explicit.agent_id)["timeout_seconds"], 480)
        service.close()

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
            replace(request, timeout_seconds=480),
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
        self.assertEqual(ADAPTER.prepare_calls, 0)
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

    def test_prepare_failure_is_durable_after_private_agent_directory(self) -> None:
        """Persist an isolated agent directory when preparation fails immediately."""
        ADAPTER.prepare_error = ValidationError("prepare exploded")
        request = self.request(request_id="prepare-failure")

        accepted = self.service.start(request)
        self.assertTrue(accepted.created)
        self.assertIn(accepted.agent.status, {AgentStatus.STARTING, AgentStatus.FAILED})
        self.wait_until(
            lambda: self.store.get_agent(accepted.agent_id)["status"]
            == AgentStatus.FAILED.value
        )

        rows = self.store.list_agents()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["status"], AgentStatus.FAILED.value)
        self.assertEqual(rows[0]["failure_kind"], "prepare_failed")
        path = self.root / "agents" / str(rows[0]["id"])
        self.assertTrue(path.is_dir())
        self.assertEqual(path.stat().st_mode & 0o777, 0o700)
        self.assertEqual(ADAPTER.prepare_dirs, [path])
        self.assertEqual(self.launched, [])

        retry = self.service.start(request)
        self.assertFalse(retry.created)
        self.assertEqual(str(retry.agent_id), str(rows[0]["id"]))
        self.assertEqual(ADAPTER.prepare_calls, 1)

    def test_launch_failure_is_durable_and_idempotent_retry_never_relaunches(self) -> None:
        """Persist immediate launch failure and keep idempotent retries side-effect free."""
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
        accepted = service.start(request)
        self.assertTrue(accepted.created)
        self.assertIn(accepted.agent.status, {AgentStatus.STARTING, AgentStatus.FAILED})
        self.wait_until(
            lambda: self.store.get_agent(accepted.agent_id)["status"]
            == AgentStatus.FAILED.value
        )

        row = self.store.list_agents()[0]
        agent_id = row["id"]
        self.assertEqual(row["status"], "failed")
        self.assertEqual(row["failure_kind"], "supervisor_start_failed")
        self.assertEqual(row["failure_text"], "ready failed")
        view = service.get(agent_id)
        # The start carried no orchestrator session reference, so no notice was
        # created: nothing could ever bind to deliver it.
        self.assertEqual(view.delivery.state, "not_created")
        self.assertIsNone(
            self.store.connection.execute(
                "SELECT id FROM deliveries WHERE agent_id = ?", (agent_id,)
            ).fetchone()
        )
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
        service.close()

    def test_bootstrap_failure_keeps_the_agent_id_stage_and_evidence_on_the_error(
        self,
    ) -> None:
        def fail_launch(*args) -> None:
            raise SupervisorBootstrapError(
                "detached supervisor died before session proof at stage "
                "'import': ModuleNotFoundError: no module named agent_run.adapters "
                "(exit code 1)",
                failure_kind=FAILURE_KIND_BOOTSTRAP,
                failure_stage="import",
                bootstrap_error_type="ModuleNotFoundError",
                bootstrap_traceback="Traceback (most recent call last):\n...\n",
                provisional_pid=999999,
                proven=False,
            )

        service = AgentService(
            self.config,
            self.store,
            self.root,
            launch=fail_launch,
            now=lambda: 100.0,
        )
        request = self.request(request_id="bootstrap-failure")
        accepted = service.start(request)
        self.assertIs(accepted.agent.status, AgentStatus.STARTING)
        self.wait_until(
            lambda: self.store.get_agent(accepted.agent_id)["status"]
            == AgentStatus.FAILED.value
        )

        row = self.store.list_agents()[0]
        agent_id = row["id"]
        self.assertEqual(str(accepted.agent_id), str(agent_id))
        self.assertEqual(row["failure_kind"], FAILURE_KIND_BOOTSTRAP)
        self.assertIn("no module named agent_run.adapters", row["failure_text"])
        event = self.store.connection.execute(
            """SELECT kind, data_json FROM events
               WHERE agent_id = ? ORDER BY seq DESC LIMIT 1""",
            (agent_id,),
        ).fetchone()
        self.assertIn('"stage":"import"', event["data_json"])
        self.assertIn('"type":"ModuleNotFoundError"', event["data_json"])
        self.assertIn('"provisional_pid":999999', event["data_json"])
        self.assertIn('"proven":false', event["data_json"])
        service.close()

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

    def test_list_orchestrators_and_agent_effort_are_read_only(self) -> None:
        """Expose persisted effort and bounded orchestrator aggregates."""

        result = self.service.start(replace(self.request(request_id="effort"), effort="high"))
        self.wait_until(lambda: bool(self.launched))
        self.assertEqual(self.service.get(result.agent_id).effort, "high")
        self.assertEqual(self.service.list(AgentQuery(limit=1)).items[0].effort, "high")
        page = self.service.list_orchestrators(limit=1)
        self.assertEqual((page.total, len(page.items), page.complete), (1, 1, True))
        with self.assertRaisesRegex(ValidationError, "limit must not exceed 1000"):
            self.service.list_orchestrators(limit=1001)

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
        self.assertTrue(directory.is_dir())
        path = directory / "answer.md"
        body = b"sealed answer"
        path.write_bytes(body)
        digest = hashlib.sha256(body).hexdigest()
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
        fake_roster = self.service.models()["fake"]
        self.assertEqual(fake_roster.models[0].id, "model")
        self.assertEqual(
            fake_roster.capabilities, tuple(sorted(c.value for c in Capability))
        )
        self.assertTrue(fake_roster.available)
        self.assertIsNone(fake_roster.reason)
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

    def test_delivery_view_exposes_only_the_latest_typed_attempt_evidence(self) -> None:
        """Expose latest safe evidence additively after a completed delivery."""

        agent_id = self.start("delivery-evidence").agent_id
        self.service.bind(
            agent_id, OrchestratorRef("codex_queue", "session-1", "turn-1")
        )
        self.terminal(agent_id)
        claimed = self.store.claim_delivery("worker", at=102, lease_seconds=10)
        evidence = DeliveryAttemptEvidence(
            "success", "/bin/codex", ("executable", "queue"), 2,
            returncode=0, message_id_present=True,
        )
        self.store.complete_delivery(
            claimed["id"], "worker", at=103, evidence=evidence
        )

        self.assertEqual(self.service.get(agent_id).delivery.last_attempt, evidence)

    def test_empty_roster_still_lists_the_runtime_with_a_reason(self) -> None:
        ADAPTER.models_result = ()

        roster = self.service.models()["fake"]

        self.assertEqual(roster.models, ())
        self.assertFalse(roster.available)
        self.assertEqual(roster.reason, "roster empty")

    def test_empty_roster_prefers_the_adapters_own_unavailable_reason(self) -> None:
        ADAPTER.models_result = ()
        ADAPTER.probe_health = RuntimeHealth(False, None, None, "no network route")

        roster = self.service.models()["fake"]

        self.assertEqual(roster.models, ())
        self.assertFalse(roster.available)
        self.assertEqual(roster.reason, "no network route")

    def test_codex_models_bootstrap_from_config_without_isolated_cache(self) -> None:
        from agent_run.config import RuntimeAuthConfig

        codex_home = self.root / "codex-runtime"
        codex_home.mkdir()
        auth = self.root / "codex-auth.json"
        auth.write_text("{}", encoding="utf-8")
        config = replace(
            self.config,
            runtimes={
                "codex": RuntimeConfig(
                    True,
                    "agent_run.adapters.codex.adapter:ADAPTER",
                    Path("/bin/true"),
                    codex_home,
                    ("gpt-5.6-sol", "gpt-5.6-terra"),
                    auth=RuntimeAuthConfig("file_link", auth, "auth.json"),
                )
            },
        )
        service = AgentService(
            config,
            self.store,
            self.root,
            launch=lambda *_: None,
            now=lambda: 100.0,
        )

        roster = service.models()["codex"]

        self.assertEqual(
            [(model.id, model.description, model.efforts) for model in roster.models],
            [
                ("gpt-5.6-sol", "", ()),
                ("gpt-5.6-terra", "", ()),
            ],
        )
        from agent_run.adapters.codex.adapter import ADAPTER as codex_adapter

        self.assertEqual(
            roster.capabilities,
            tuple(sorted(c.value for c in codex_adapter.describe().capabilities)),
        )
        self.assertFalse(roster.available)
        self.assertEqual(
            roster.reason, "codex binary, generated home, or auth bridge is missing"
        )
        self.assertFalse((codex_home / "cache" / "models.json").exists())

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
            self.wait_until(lambda: len(launched) == 1)
            self.assertEqual(len(launched), 1)
        finally:
            service.close()


if __name__ == "__main__":
    unittest.main()
