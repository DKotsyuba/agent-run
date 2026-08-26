import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from agent_run.config import CapacityConfig, Config
from agent_run.domain import AgentStatus, OrchestratorRef, StartRequest
from agent_run.hooks.context import (
    ACTIVE_BLOCK_MAX_CHARS,
    CONTEXT_HARD_LIMIT_CHARS,
    build_context,
)
from agent_run.state import StateStore


class ContextHookTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name).resolve()
        self.store = StateStore.initialize(self.root / "state.db")
        self.config = Config(schema_version=1)
        self.ref = OrchestratorRef("codex_queue", "session-1", "turn-1")

    def tearDown(self) -> None:
        self.store.close()
        self.temporary.cleanup()

    def request(self) -> StartRequest:
        return StartRequest(
            "codex", "model", "profile", "do work", self.root, orchestrator=self.ref
        )

    def test_first_prompt_creates_receipt_dedups_and_reuses_later_binding(self) -> None:
        first = build_context(self.store, self.ref, config=self.config, now=1000.0)
        self.assertIsNotNone(first.orchestrator_session_id)
        self.assertEqual(
            self.store.find_orchestrator_session(self.ref), first.orchestrator_session_id
        )
        self.assertEqual(self.store.list_agents(), [])
        self.assertTrue(first.injected)
        self.assertLessEqual(len(first.text), CONTEXT_HARD_LIMIT_CHARS)
        self.assertIn("Capacity: unknown.", first.text)

        second = build_context(self.store, self.ref, config=self.config, now=1001.0)
        self.assertFalse(second.injected)
        self.assertEqual(second.text, "")
        self.assertEqual(second.orchestrator_session_id, first.orchestrator_session_id)

        agent_id = self.store.create_agent(
            self.request(), task_summary="summary", config_revision="cfg-1", at=2
        ).agent_id
        self.assertEqual(
            self.store.bind_orchestrator(agent_id, self.ref, at=3),
            first.orchestrator_session_id,
        )

    def test_bounds_and_dedup_key_is_stable_until_agent_state_changes(self) -> None:
        agent_id = self.store.create_agent(
            self.request(),
            task_summary="summary\n" + "x" * 100,
            config_revision="cfg-1",
            at=1,
        ).agent_id
        self.store.bind_orchestrator(agent_id, self.ref, at=1)
        self.store.transition(agent_id, AgentStatus.STARTING, at=2)
        self.store.transition(agent_id, AgentStatus.RUNNING, at=3)

        first = build_context(self.store, self.ref, config=self.config, now=1000.0)
        self.assertIsNotNone(first.orchestrator_session_id)
        self.assertTrue(first.injected)
        self.assertIn("Active agents (1)", first.text)
        self.assertLessEqual(len(first.text), CONTEXT_HARD_LIMIT_CHARS)
        active_block = first.text.splitlines()[-1]
        self.assertLessEqual(len(active_block), ACTIVE_BLOCK_MAX_CHARS)
        self.assertIn("summary", active_block)
        self.assertIn("do not start replacements for existing ids", active_block)

        # Same session, only elapsed time moved forward a little: unchanged, so it
        # must not be re-injected and the receipt is not rewritten.
        second = build_context(self.store, self.ref, config=self.config, now=1030.0)
        self.assertFalse(second.injected)
        self.assertEqual(second.text, "")
        self.assertEqual(second.context_key, first.context_key)

        # A material state change (terminal transition) must change the key and
        # trigger re-injection.
        self.store.transition(agent_id, AgentStatus.SUCCEEDED, at=1040)
        third = build_context(self.store, self.ref, config=self.config, now=1041.0)
        self.assertTrue(third.injected)
        self.assertNotEqual(third.context_key, first.context_key)
        self.assertNotIn("Active agents", third.text)

    def test_context_budget_is_never_exceeded_with_many_active_agents(self) -> None:
        for index in range(12):
            request = StartRequest(
                "codex",
                "model",
                "profile",
                f"task {index}",
                self.root,
                orchestrator=self.ref,
                request_id=f"req-{index}",
            )
            agent_id = self.store.create_agent(
                request, task_summary="summary", config_revision="cfg-1", at=index
            ).agent_id
            self.store.bind_orchestrator(agent_id, self.ref, at=index)
            self.store.transition(agent_id, AgentStatus.STARTING, at=index)
            self.store.transition(agent_id, AgentStatus.RUNNING, at=index)

        result = build_context(self.store, self.ref, config=self.config, now=2000.0)
        self.assertIn("Active agents (12)", result.text)
        self.assertIn("more", result.text)
        self.assertLessEqual(len(result.text), CONTEXT_HARD_LIMIT_CHARS)
        active_block = result.text.splitlines()[-1]
        self.assertLessEqual(len(active_block), ACTIVE_BLOCK_MAX_CHARS)
        self.assertIn("do not start replacements for existing ids", active_block)

    def test_configured_budget_below_hard_limit_is_respected(self) -> None:
        agent_id = self.store.create_agent(
            self.request(), task_summary="summary", config_revision="cfg-1", at=1
        ).agent_id
        self.store.bind_orchestrator(agent_id, self.ref, at=1)
        tight_config = Config(schema_version=1, capacity=CapacityConfig(context_max_chars=80))
        result = build_context(self.store, self.ref, config=tight_config, now=1000.0)
        self.assertLessEqual(len(result.text), 80)

    def test_non_nominal_capacity_labels_and_truncation(self) -> None:
        self.store.insert_capacity_sample(
            runtime="codex",
            lane="requests",
            window="5h",
            source="provider",
            target="model-a",
            payload={},
            remaining_percent=0,
            reset_at=2000,
            observed_at=1000,
            valid_until=1100,
        )
        first = build_context(self.store, self.ref, config=self.config, now=1000)
        self.assertIn("risk=high", first.text)
        self.assertIn("target=model-a", first.text)
        self.assertIn("source=provider", first.text)

        for index in range(20):
            self.store.insert_capacity_sample(
                runtime="codex",
                lane=f"lane-{index}",
                window="5h",
                source=f"source-{index}",
                target="model-with-a-long-identity",
                payload={},
                remaining_percent=5,
                reset_at=2000,
                observed_at=1001,
                valid_until=1100,
            )
        tight = Config(schema_version=1, capacity=CapacityConfig(context_max_chars=120))
        truncated = build_context(self.store, self.ref, config=tight, now=1001)
        self.assertTrue(truncated.injected)
        self.assertLessEqual(len(truncated.text), 120)


if __name__ == "__main__":
    unittest.main()
