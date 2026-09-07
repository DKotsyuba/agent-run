"""Durable resume storage and AgentService admission."""

from __future__ import annotations

import os
import json
import sqlite3
import tempfile
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from pathlib import Path
from threading import Barrier, Event
from unittest.mock import patch

from agent_run.adapters.base import (
    ADAPTER_API_VERSION,
    Capability,
    LaunchPlan,
    ModelInfo,
    RuntimeHealth,
    RuntimeInfo,
)
from agent_run.adapters.snapshots import finalize_runtime_snapshots
from agent_run.config import Config, ProfilesConfig, RuntimeAuthConfig, RuntimeConfig
from agent_run.domain import AgentStatus, OrchestratorRef, Outcome, StartRequest
from agent_run.errors import ValidationError
from agent_run.service import AgentService
from agent_run.state.store import StateStore


class ResumableAdapter:
    """Fake runtime that declares RESUME and records the plans it produced."""

    def __init__(self) -> None:
        self.capabilities = frozenset(Capability)
        self.materialize_calls = 0
        self.prepare_calls = 0
        self.prepare_homes = []

    def describe(self) -> RuntimeInfo:
        return RuntimeInfo("fake", ADAPTER_API_VERSION, self.capabilities)

    def validate(self, config) -> None:
        """Accept any configuration; capability gating is the service's job."""

    def materialize(self, config, home, *, mcp_servers, skills_root) -> str:
        """Finalize an empty managed index and return its fixed revision."""

        self.materialize_calls += 1
        finalize_runtime_snapshots(Path(home), "cfg-1")
        return "cfg-1"

    def probe(self, config, home) -> RuntimeHealth:
        """Report a healthy runtime so model discovery is reached."""

        return RuntimeHealth(True, "1", True, None)

    def models(self, config, home) -> tuple[ModelInfo, ...]:
        """Return the single roster entry every test request asks for."""

        return (ModelInfo("model", "fake model", ("high",)),)

    def limits(self, config, home):
        """Never called: the service reads stored capacity samples instead."""

        raise AssertionError("service limits must use stored samples")

    def prepare(self, request, profile, config, home, agent_dir, *, mcp_servers):
        """Build a plan with no resume identity, as a real adapter would."""

        self.prepare_calls += 1
        self.prepare_homes.append(Path(home))
        return LaunchPlan(
            ("fake",), request.workdir, {}, request.task,
            agent_dir / "runtime.jsonl", {}, agent_dir / "answer.md",
        )

    def launch(self, plan, sink):
        """Never called: AgentService launches through an injected seam."""

        raise AssertionError("AgentService uses the injected launch seam")


ADAPTER = ResumableAdapter()


class ResumeTests(unittest.TestCase):
    def test_replay_survives_deleted_paths_and_changed_runtime(self) -> None:
        """An accepted request is immutable even after mutable resources disappear."""
        parent = self._parent()
        first = self.service.resume(parent, "continue", request_id="durable-replay")
        self._wait(2)
        self.workdir.rmdir()
        moved = replace(self.config.runtimes["fake"], home=self.root / "changed-home")
        self.service = self._service(replace(self.config, runtimes={"fake": moved}))
        again = self.service.resume(parent, "continue", request_id="durable-replay")
        self.assertEqual(first.agent_id, again.agent_id)
        self.assertFalse(again.created)
        with self.assertRaisesRegex(ValidationError, "request_id"):
            self.service.resume(parent, "different", request_id="durable-replay")

    def test_replay_preserves_notification_caller_and_validates_timeout(self) -> None:
        """Early replay checks caller identity and does not accept boolean timeouts."""
        parent = self._parent()
        caller = OrchestratorRef("codex_queue", "caller-one")
        first = self.service.resume(parent, "continue", request_id="caller-replay", orchestrator=caller)
        self._wait(2)
        again = self.service.resume(parent, "continue", request_id="caller-replay", orchestrator=caller)
        self.assertEqual(first.agent_id, again.agent_id)
        with self.assertRaisesRegex(ValidationError, "already been resumed"):
            self.service.resume(parent, "continue", request_id="caller-replay")
        with self.assertRaises(ValidationError):
            self.service.resume(parent, "continue", request_id="caller-replay", timeout_seconds=True)

    def test_profile_drift_between_admission_and_preparation_cannot_launch(self) -> None:
        """The worker checks actual grants after loading a concurrently changed profile."""
        parent = self._parent()
        with patch.object(self.service._starts, "submit") as submit:
            child = self.service.resume(parent, "continue")
        (self.profiles / "profile.md").write_text(
            "+++\nwrite = true\nnetwork = true\n+++\nDo the requested work.\n"
        )
        submit.call_args.args[1](self.store, Event())
        self.assertEqual(self.service.get(child.agent_id).status, AgentStatus.FAILED)
        self.assertIn("profile grants", self.service.get(child.agent_id).failure_text)
        self.assertEqual(len(self.launched), 1)

    def test_unchanged_network_grants_are_preserved(self) -> None:
        """A legitimately network-enabled parent remains resumable with the same grants."""
        (self.profiles / "profile.md").write_text(
            "+++\nwrite = true\nnetwork = true\n+++\nDo the requested work.\n"
        )
        parent = self._parent()
        child = self.service.resume(parent, "continue")
        self._wait(2)
        snapshot = json.loads(self.store.get_agent(child.agent_id)["identity_json"])
        self.assertTrue(snapshot["profile_grants"]["network"])

    """End-to-end resume admission over a real store and the fake adapter."""

    def setUp(self) -> None:
        ADAPTER.__init__()
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
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
        self.service = self._service(self.config)

    def _service(self, config: Config) -> AgentService:
        """Build a service over this test's store with a recording launch seam."""

        service = AgentService(
            config,
            self.store,
            self.root,
            launch=lambda *args: self.launched.append(args),
            now=lambda: 100.0,
        )
        self.addCleanup(service.close)
        return service

    def _wait(self, count: int) -> LaunchPlan:
        """Wait for the ``count``-th launch and return the plan it received."""

        deadline = time.monotonic() + 2.0
        while time.monotonic() < deadline:
            if len(self.launched) >= count:
                return self.launched[count - 1][3]
            time.sleep(0.01)
        self.fail("launch did not happen")

    def _finish(self, agent_id, session="sess-1", status=AgentStatus.SUCCEEDED):
        """Drive one accepted agent to a terminal status with a native session."""

        self.store.transition(
            agent_id, AgentStatus.RUNNING, kind="running", at=101.0
        )
        self.store.transition(
            agent_id,
            status,
            outcome=Outcome(status, runtime_session_id=session),
            at=102.0,
        )

    def _parent(self, *, session="sess-1", request_id=None, task="do work"):
        """Start one agent, wait for its launch, and finish it as a source."""

        target = len(self.launched) + 1
        result = self.service.start(
            StartRequest("fake", "model", "profile", task, self.workdir,
                         request_id=request_id)
        )
        self._wait(target)
        self._finish(result.agent_id, session=session)
        return result.agent_id

    # -- happy path ----------------------------------------------------

    def test_resume_makes_a_new_agent_that_inherits_and_attaches(self) -> None:
        parent = self._parent()
        answer = self.root / "agents" / parent / "answer.md"

        result = self.service.resume(parent, "keep going")
        plan = self._wait(2)

        self.assertTrue(result.created)
        self.assertNotEqual(result.agent_id, parent)
        # The adapter is asked to attach to exactly the parent's session, and
        # only after prepare produced a plan without one.
        self.assertEqual(plan.resume_session_id, "sess-1")
        child = self.service.get(result.agent_id)
        self.assertEqual(child.parent_agent_id, parent)
        self.assertEqual(child.root_agent_id, parent)
        self.assertEqual(child.sequence, 2)
        self.assertEqual(child.task_summary, "keep going")
        # Inherited identity, and a fresh answer path of its own.
        self.assertEqual((child.runtime, child.model, child.profile),
                         ("fake", "model", "profile"))
        self.assertEqual(
            float(self.store.get_agent(result.agent_id)["timeout_seconds"]),
            float(self.store.get_agent(parent)["timeout_seconds"]),
        )
        self.assertNotEqual(plan.answer_path, answer)
        # The parent's own row is untouched by the resume.
        self.assertEqual(self.service.get(parent).status, AgentStatus.SUCCEEDED)
        self.assertIsNone(self.service.get(parent).parent_agent_id)

    def test_explicit_timeout_overrides_and_omitted_one_inherits(self) -> None:
        parent = self._parent()
        child = self.service.resume(parent, "keep going", timeout_seconds=7.5)
        self._wait(2)
        self.assertEqual(
            float(self.store.get_agent(child.agent_id)["timeout_seconds"]), 7.5
        )

    # -- refusals ------------------------------------------------------

    def test_unfinished_parent_is_refused(self) -> None:
        result = self.service.start(
            StartRequest("fake", "model", "profile", "do work", self.workdir)
        )
        self._wait(1)
        with self.assertRaises(ValidationError) as caught:
            self.service.resume(result.agent_id, "keep going")
        self.assertIn("not resumable", str(caught.exception))

    def test_parent_without_a_native_session_is_refused(self) -> None:
        parent = self._parent()
        self.store.connection.execute(
            "UPDATE agents SET runtime_session_id = NULL WHERE id = ?", (parent,)
        )
        self.store.connection.commit()
        with self.assertRaises(ValidationError) as caught:
            self.service.resume(parent, "keep going")
        self.assertIn("no runtime session", str(caught.exception))

    def test_finished_parent_whose_process_group_still_lives_is_refused(self) -> None:
        """A terminal row is not proof the runtime released its session."""

        parent = self._parent()
        self.store.connection.execute(
            "UPDATE agents SET process_group_id = ? WHERE id = ?",
            (os.getpgid(os.getpid()), parent),
        )
        self.store.connection.commit()
        with self.assertRaises(ValidationError) as caught:
            self.service.resume(parent, "keep going")
        self.assertIn("still", str(caught.exception))

    def test_lost_parent_is_resumable_only_once_quiescence_is_proven(self) -> None:
        parent = self._parent(session="sess-lost")
        self.store.connection.execute(
            """UPDATE agents SET status = 'lost', supervisor_pid = 4242,
               process_group_id = NULL WHERE id = ?""",
            (parent,),
        )
        self.store.connection.commit()
        with self.assertRaises(ValidationError):
            self.service.resume(parent, "keep going")

        # A pid we can prove is gone makes the same row a legitimate source.
        self.store.connection.execute(
            "UPDATE agents SET process_group_id = 999999999 WHERE id = ?", (parent,)
        )
        self.store.connection.commit()
        self.service.resume(parent, "keep going")
        self.assertEqual(self._wait(2).resume_session_id, "sess-lost")

    def test_missing_inherited_workdir_refuses_instead_of_recreating(self) -> None:
        parent = self._parent()
        for entry in sorted(self.workdir.rglob("*"), reverse=True):
            entry.unlink()
        self.workdir.rmdir()
        with self.assertRaises(ValidationError):
            self.service.resume(parent, "keep going")
        self.assertFalse(self.workdir.exists())

    def test_legacy_row_without_an_identity_snapshot_is_refused(self) -> None:
        parent = self._parent()
        self.store.connection.execute(
            "UPDATE agents SET identity_json = NULL WHERE id = ?", (parent,)
        )
        self.store.connection.commit()
        with self.assertRaises(ValidationError) as caught:
            self.service.resume(parent, "keep going")
        self.assertIn("identity", str(caught.exception))

    # -- account and configuration drift --------------------------------

    def _accounted(self, *, accounts, default) -> Config:
        """Return this test's config with an account-bearing fake runtime."""

        runtime = self.config.runtimes["fake"]
        return replace(self.config, runtimes={"fake": replace(
            runtime,
            auth=RuntimeAuthConfig("file", target="auth.json"),
            accounts=accounts,
            default_account=default,
        )})

    def _authenticate(self, label: str) -> None:
        """Write the account credential file the service checks before launch."""

        source = self.root / "accounts" / "fake" / label / "auth.json"
        source.parent.mkdir(parents=True, exist_ok=True)
        source.write_text("{}", encoding="utf-8")

    def test_unlabelled_base_account_stays_resumable_after_default_changes(self) -> None:
        """account=None recorded with a snapshot is proof, not legacy unknown."""

        self.service = self._service(self._accounted(accounts=("work",), default=None))
        parent = self._parent()

        # The default now points somewhere else; the snapshot must still win.
        self.service = self._service(
            self._accounted(accounts=("work",), default="work")
        )
        self._authenticate("work")
        child = self.service.resume(parent, "keep going")
        plan = self._wait(2)
        self.assertEqual(plan.resume_session_id, "sess-1")
        row = self.store.get_agent(child.agent_id)
        self.assertIn('"account":null', str(row["identity_json"]))

    def test_explicit_label_survives_a_changed_default_account(self) -> None:
        self.service = self._service(
            self._accounted(accounts=("work", "other"), default="work")
        )
        self._authenticate("work")
        parent = self._parent()

        self.service = self._service(
            self._accounted(accounts=("work", "other"), default="other")
        )
        self.service.resume(parent, "keep going")
        self._wait(2)
        self.assertIn(
            '"account":"work"',
            str(self.store.get_agent(self.service.chain(parent).items[1].agent_id)[
                "identity_json"
            ]),
        )

    def test_account_no_longer_declared_is_refused(self) -> None:
        self.service = self._service(
            self._accounted(accounts=("work",), default="work")
        )
        self._authenticate("work")
        parent = self._parent()

        self.service = self._service(
            self._accounted(accounts=("other",), default="other")
        )
        with self.assertRaises(ValidationError) as caught:
            self.service.resume(parent, "keep going")
        self.assertIn("no longer declared", str(caught.exception))

    def test_runtime_home_drift_is_refused(self) -> None:
        parent = self._parent()
        moved = self.root / "elsewhere"
        moved.mkdir()
        self.service = self._service(replace(self.config, runtimes={
            "fake": replace(self.config.runtimes["fake"], home=moved)
        }))
        with self.assertRaises(ValidationError) as caught:
            self.service.resume(parent, "keep going")
        self.assertIn("identity changed", str(caught.exception))

    def test_model_removed_from_configuration_is_refused(self) -> None:
        parent = self._parent()
        self.service = self._service(replace(self.config, runtimes={
            "fake": replace(self.config.runtimes["fake"], models=("other",))
        }))
        with self.assertRaises(ValidationError) as caught:
            self.service.resume(parent, "keep going")
        self.assertIn("no longer configured", str(caught.exception))

    def test_runtime_without_resume_capability_is_refused(self) -> None:
        parent = self._parent()
        ADAPTER.capabilities = frozenset(Capability) - {Capability.RESUME}
        with self.assertRaises(ValidationError):
            self.service.resume(parent, "keep going")

    # -- idempotency, races, chains --------------------------------------

    def test_snapshot_resume_reuses_root_home_without_rematerializing(self) -> None:
        """Every continuation verifies and reuses its root lineage runtime home."""

        parent = self._parent(session="sess-1")
        root_home = self.root / "agents" / parent / "runtime-home"
        revision = self.store.get_agent(parent)["config_revision"]
        self.assertTrue(str(revision).startswith("snapshot:v1:"))
        self.assertEqual(ADAPTER.materialize_calls, 1)

        second = self.service.resume(parent, "second")
        self._wait(2)
        self.assertEqual(ADAPTER.materialize_calls, 1)
        self.assertEqual(ADAPTER.prepare_homes[-1], root_home)
        self.assertEqual(
            self.store.get_agent(second.agent_id)["config_revision"], revision
        )
        self._finish(second.agent_id, session="sess-2")

        third = self.service.resume(second.agent_id, "third")
        self._wait(3)
        self.assertEqual(ADAPTER.materialize_calls, 1)
        self.assertEqual(ADAPTER.prepare_homes[-1], root_home)
        self.assertEqual(
            self.store.get_agent(third.agent_id)["config_revision"], revision
        )

    def test_missing_new_lineage_home_never_falls_back_to_legacy(self) -> None:
        """A prefixed parent fails closed when its authoritative HOME is absent."""

        parent = self._parent()
        runtime_home = self.root / "agents" / parent / "runtime-home"
        runtime_home.rename(runtime_home.with_name("runtime-home-missing"))

        child = self.service.resume(parent, "continue")
        deadline = time.monotonic() + 2
        while self.service.get(child.agent_id).status not in {
            AgentStatus.FAILED,
            AgentStatus.CANCELLED,
        }:
            self.assertLess(time.monotonic(), deadline)
            time.sleep(0.01)

        view = self.service.get(child.agent_id)
        self.assertIs(view.status, AgentStatus.FAILED)
        self.assertIn("snapshot", view.failure_text)
        self.assertEqual(ADAPTER.materialize_calls, 1)
        self.assertEqual(len(self.launched), 1)

    def test_same_request_id_replays_the_same_child_even_when_stale(self) -> None:
        parent = self._parent()
        first = self.service.resume(parent, "keep going", request_id="rq-1")
        self._wait(2)
        again = self.service.resume(parent, "keep going", request_id="rq-1")
        self.assertEqual(again.agent_id, first.agent_id)
        self.assertFalse(again.created)
        self.assertEqual(len(self.launched), 2)

    def test_same_request_id_against_a_different_parent_conflicts(self) -> None:
        first_parent = self._parent(request_id="p-1")
        second_parent = self._parent(request_id="p-2", session="sess-2")
        self.service.resume(first_parent, "keep going", request_id="rq-1")
        self._wait(3)
        with self.assertRaises(ValidationError) as caught:
            self.service.resume(second_parent, "keep going", request_id="rq-1")
        self.assertIn("reused", str(caught.exception))

    def test_same_request_id_with_a_different_task_conflicts(self) -> None:
        parent = self._parent()
        self.service.resume(parent, "keep going", request_id="rq-1")
        self._wait(2)
        with self.assertRaises(ValidationError):
            self.service.resume(parent, "something else", request_id="rq-1")

    def test_a_second_resume_of_the_same_parent_names_the_winner(self) -> None:
        parent = self._parent()
        first = self.service.resume(parent, "keep going")
        self._wait(2)
        with self.assertRaises(ValidationError) as caught:
            self.service.resume(parent, "again")
        self.assertIn(first.agent_id, str(caught.exception))

    def test_stale_ancestor_names_the_latest_descendant(self) -> None:
        """An old source identifies the actual chain head, not another stale node."""
        parent = self._parent()
        second = self.service.resume(parent, "second")
        self._wait(2)
        self._finish(second.agent_id)
        third = self.service.resume(second.agent_id, "third")
        self._wait(3)
        with self.assertRaisesRegex(ValidationError, third.agent_id):
            self.service.resume(parent, "stale request")

    def test_two_processes_racing_one_parent_accept_exactly_one_child(self) -> None:
        """Two separate SQLite connections, one durable child."""

        parent = self._parent()
        database = self.store.path()
        barrier = Barrier(2)

        def attempt(suffix: str) -> str:
            from agent_run.state.start import create_agent

            connection = sqlite3.connect(database, timeout=5.0)
            connection.row_factory = sqlite3.Row
            try:
                request = StartRequest(
                    "fake", "model", "profile", f"race {suffix}", self.workdir,
                    timeout_seconds=60.0,
                )
                barrier.wait(timeout=5.0)
                create_agent(
                    connection,
                    request,
                    task_summary=f"race {suffix}",
                    config_revision="cfg-1",
                    parent_agent_id=parent,
                )
                return "accepted"
            except ValidationError as error:
                return str(error)
            finally:
                connection.close()

        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(attempt, ("a", "b")))

        self.assertEqual(results.count("accepted"), 1)
        self.assertEqual(
            self.store.connection.execute(
                "SELECT COUNT(*) FROM agents WHERE parent_agent_id = ?", (parent,)
            ).fetchone()[0],
            1,
        )

    def test_failed_child_stays_latest_and_lends_its_source_session(self) -> None:
        """A child that failed before attaching still points at the chain's
        last confirmed native context, and the next resume reuses it."""

        parent = self._parent(session="sess-1")
        child = self.service.resume(parent, "keep going")
        self._wait(2)
        self.store.transition(
            child.agent_id,
            AgentStatus.FAILED,
            outcome=Outcome(AgentStatus.FAILED, failure_kind="prepare_failed"),
            at=103.0,
        )
        row = self.store.get_agent(child.agent_id)
        self.assertIsNone(row["runtime_session_id"])
        self.assertEqual(row["resume_of_runtime_session_id"], "sess-1")

        grandchild = self.service.resume(child.agent_id, "third try")
        plan = self._wait(3)
        self.assertEqual(plan.resume_session_id, "sess-1")
        self.assertEqual(self.service.get(grandchild.agent_id).sequence, 3)
        self.assertEqual(self.service.get(grandchild.agent_id).root_agent_id, parent)

    def test_chain_pages_chronologically_from_any_member(self) -> None:
        parent = self._parent()
        second = self.service.resume(parent, "two").agent_id
        self._wait(2)
        self._finish(second, session="sess-2")
        third = self.service.resume(second, "three").agent_id
        self._wait(3)

        page = self.service.chain(third, limit=2)
        self.assertEqual([view.agent_id for view in page.items], [parent, second])
        self.assertEqual([view.sequence for view in page.items], [1, 2])
        self.assertEqual(page.next_cursor, 3)
        self.assertFalse(page.complete)

        rest = self.service.chain(parent, cursor=page.next_cursor, limit=2)
        self.assertEqual([view.agent_id for view in rest.items], [third])
        self.assertIsNone(rest.next_cursor)
        self.assertTrue(rest.complete)

    def test_a_fresh_start_is_its_own_chain_root(self) -> None:
        result = self.service.start(
            StartRequest("fake", "model", "profile", "do work", self.workdir)
        )
        self._wait(1)
        view = self.service.get(result.agent_id)
        self.assertIsNone(view.parent_agent_id)
        self.assertEqual(view.root_agent_id, result.agent_id)
        self.assertEqual(view.sequence, 1)
        self.assertIsNone(self._wait(1).resume_session_id)

    def test_start_replay_survives_a_changed_default_account(self) -> None:
        """Recording resolved identity must not change start idempotency."""

        self.service = self._service(self._accounted(accounts=("work",), default=None))
        request = StartRequest(
            "fake", "model", "profile", "do work", self.workdir, request_id="rq-1"
        )
        first = self.service.start(request)
        self._wait(1)

        self.service = self._service(
            self._accounted(accounts=("work",), default="work")
        )
        self._authenticate("work")
        again = self.service.start(request)
        self.assertEqual(again.agent_id, first.agent_id)
        self.assertFalse(again.created)


if __name__ == "__main__":
    unittest.main()
