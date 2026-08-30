import io
import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from agent_run import cli
from agent_run.domain import AgentStatus, Outcome, StartRequest
from agent_run.state.run_stats import backfill_run_stats, record_run_stats
from agent_run.state.store import StateStore

_RUNTIME_RESULT_PAYLOAD = {
    "duration_ms": 251533,
    "duration_api_ms": 240001.5,
    "num_turns": 29,
    "ttft_ms": 1234.5,
    "total_cost_usd": 0.818,
    "usage": {
        "cache_read_input_tokens": 676672,
        "cache_creation_input_tokens": 1234,
        "input_tokens": 39475,
        "output_tokens": 10812,
        "output_tokens_details": {"thinking_tokens": 4242},
    },
}

_TOKEN_USAGE_PAYLOAD = {
    "tokenUsage": {
        "total": {
            "inputTokens": 5001,
            "outputTokens": 902,
            "cachedInputTokens": 3000,
            "cacheWriteInputTokens": 700,
            "reasoningOutputTokens": 211,
            "totalTokens": 5903,
        }
    }
}


class RunStatsTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name).resolve()
        self.addCleanup(self.temporary.cleanup)
        self.store = StateStore.initialize(self.root / "state.db")
        self.addCleanup(self.store.close)

    def create_agent(self, runtime: str = "claude") -> str:
        return self.store.create_agent(
            StartRequest(runtime, "model", "profile", "task", self.root,
                         timeout_seconds=480),
            task_summary="task",
            config_revision="rev-1",
        ).agent_id

    def run_terminal(self, agent_id: str, *, at: float = 100.0) -> None:
        self.store.transition(
            agent_id, AgentStatus.STARTING, kind="supervisor_starting", at=at - 1.0
        )
        self.store.transition(agent_id, AgentStatus.RUNNING, kind="running", at=at)
        self.store.transition(
            agent_id,
            AgentStatus.SUCCEEDED,
            outcome=Outcome(AgentStatus.SUCCEEDED),
            kind="terminal",
            at=at + 10.0,
        )

    def stats_row(self, agent_id: str) -> dict:
        row = self.store.connection.execute(
            "SELECT * FROM run_stats WHERE agent_id = ?", (agent_id,)
        ).fetchone()
        self.assertIsNotNone(row)
        return dict(row)

    def test_runtime_result_payload_normalizes_the_claude_family_shape(self) -> None:
        agent_id = self.create_agent("claude")
        self.run_terminal(agent_id)
        self.store.append_event(agent_id, "runtime_result", data=_RUNTIME_RESULT_PAYLOAD)

        row = record_run_stats(self.store, agent_id)

        self.assertEqual(row["usage_source"], "runtime_result")
        self.assertEqual(row["status"], "succeeded")
        self.assertEqual(row["runtime"], "claude")
        self.assertEqual(row["model"], "model")
        self.assertEqual(row["profile"], "profile")
        self.assertEqual(row["started_at"], 100.0)
        self.assertEqual(row["finished_at"], 110.0)
        self.assertEqual(row["duration_seconds"], 10.0)
        self.assertEqual(row["input_tokens"], 39475)
        self.assertEqual(row["output_tokens"], 10812)
        self.assertEqual(row["cache_read_tokens"], 676672)
        self.assertEqual(row["cache_write_tokens"], 1234)
        self.assertEqual(row["reasoning_tokens"], 4242)
        self.assertEqual(row["num_turns"], 29)
        self.assertEqual(row["ttft_ms"], 1234.5)
        self.assertEqual(row["api_duration_ms"], 240001.5)
        self.assertEqual(row["cost_usd"], 0.818)
        # runtime_result reports no grand total; absence stays NULL, not zero.
        self.assertIsNone(row["total_tokens"])

    def test_last_token_usage_update_normalizes_the_codex_shape(self) -> None:
        agent_id = self.create_agent("codex")
        self.run_terminal(agent_id)
        self.store.append_event(
            agent_id,
            "thread/tokenUsage/updated",
            data={"tokenUsage": {"total": {"inputTokens": 1, "totalTokens": 2}}},
        )
        self.store.append_event(
            agent_id, "thread/tokenUsage/updated", data=_TOKEN_USAGE_PAYLOAD
        )

        row = record_run_stats(self.store, agent_id)

        self.assertEqual(row["usage_source"], "token_usage_updated")
        self.assertEqual(row["input_tokens"], 5001)
        self.assertEqual(row["output_tokens"], 902)
        self.assertEqual(row["cache_read_tokens"], 3000)
        self.assertEqual(row["cache_write_tokens"], 700)
        self.assertEqual(row["reasoning_tokens"], 211)
        self.assertEqual(row["total_tokens"], 5903)
        # codex reports no turns, ttft, api duration, or cost.
        self.assertIsNone(row["num_turns"])
        self.assertIsNone(row["ttft_ms"])
        self.assertIsNone(row["api_duration_ms"])
        self.assertIsNone(row["cost_usd"])

    def test_an_agent_without_usage_events_records_all_nulls(self) -> None:
        agent_id = self.create_agent("claude")
        self.run_terminal(agent_id)

        row = record_run_stats(self.store, agent_id)

        self.assertEqual(row["usage_source"], "none")
        for field in (
            "input_tokens", "output_tokens", "cache_read_tokens",
            "cache_write_tokens", "reasoning_tokens", "total_tokens",
            "num_turns", "ttft_ms", "api_duration_ms", "cost_usd",
        ):
            self.assertIsNone(row[field], field)
        self.assertEqual(row["duration_seconds"], 10.0)

    def test_a_failed_run_keeps_its_failure_kind_and_timestamps(self) -> None:
        agent_id = self.create_agent("codex")
        self.store.transition(
            agent_id, AgentStatus.STARTING, kind="supervisor_starting", at=50.0
        )
        self.store.transition(agent_id, AgentStatus.RUNNING, kind="running", at=51.0)
        self.store.transition(
            agent_id,
            AgentStatus.FAILED,
            outcome=Outcome(
                AgentStatus.FAILED, failure_kind="stalled", failure_text="no stream"
            ),
            kind="terminal",
            at=61.0,
        )

        row = record_run_stats(self.store, agent_id)

        self.assertEqual(row["status"], "failed")
        self.assertEqual(row["failure_kind"], "stalled")
        self.assertEqual(row["started_at"], 51.0)
        self.assertEqual(row["finished_at"], 61.0)
        self.assertEqual(row["duration_seconds"], 10.0)

    def test_a_created_agent_without_transitions_has_null_timestamps(self) -> None:
        agent_id = self.create_agent("claude")

        row = record_run_stats(self.store, agent_id)

        self.assertEqual(row["status"], "created")
        self.assertEqual(row["usage_source"], "none")
        self.assertIsNone(row["started_at"])
        self.assertIsNone(row["finished_at"])
        self.assertIsNone(row["duration_seconds"])
        self.assertIsNotNone(row["recorded_at"])

    def test_recording_replaces_the_row_idempotently(self) -> None:
        agent_id = self.create_agent("claude")
        self.run_terminal(agent_id)

        first = record_run_stats(self.store, agent_id)
        self.store.append_event(agent_id, "runtime_result", data=_RUNTIME_RESULT_PAYLOAD)
        second = record_run_stats(self.store, agent_id)

        self.assertEqual(first["usage_source"], "none")
        self.assertEqual(second["usage_source"], "runtime_result")
        self.assertEqual(
            self.store.connection.execute(
                "SELECT COUNT(*) FROM run_stats WHERE agent_id = ?", (agent_id,)
            ).fetchone()[0],
            1,
        )

    def test_malformed_event_payloads_are_skipped_not_fatal(self) -> None:
        agent_id = self.create_agent("claude")
        self.run_terminal(agent_id)
        self.store.connection.execute(
            "INSERT INTO events (agent_id, at, kind, data_json) VALUES (?, 1.0, 'runtime_result', 'not json')"
            , (agent_id,),
        )
        self.store.connection.commit()

        row = record_run_stats(self.store, agent_id)

        self.assertEqual(row["usage_source"], "none")

    def test_backfill_fills_missing_rows_and_is_idempotent(self) -> None:
        claude = self.create_agent("claude")
        codex = self.create_agent("codex")
        self.run_terminal(claude)
        self.store.append_event(claude, "runtime_result", data=_RUNTIME_RESULT_PAYLOAD)
        record_run_stats(self.store, codex)

        first = backfill_run_stats(self.store)
        self.assertEqual(first, {"backfilled": 1, "skipped": 0})
        self.assertEqual(self.stats_row(claude)["input_tokens"], 39475)

        second = backfill_run_stats(self.store)
        self.assertEqual(second, {"backfilled": 0, "skipped": 0})

    def test_backfill_counts_a_per_agent_failure_as_skipped(self) -> None:
        import agent_run.state.run_stats as run_stats

        agent_id = self.create_agent("claude")
        original = run_stats.record_run_stats

        def failing(store, aid, *, at=None):
            raise RuntimeError("boom")

        run_stats.record_run_stats = failing
        try:
            with self.assertLogs("agent_run.state", level="WARNING"):
                result = backfill_run_stats(self.store)
        finally:
            run_stats.record_run_stats = original
        self.assertEqual(result, {"backfilled": 0, "skipped": 1})
        self.assertEqual(
            self.store.connection.execute(
                "SELECT COUNT(*) FROM run_stats WHERE agent_id = ?", (agent_id,)
            ).fetchone()[0],
            0,
        )


class CliStatsBackfillTests(unittest.TestCase):
    def test_stats_backfill_verb_prints_counts_and_is_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            home = Path(directory).resolve()
            (home / "config.toml").write_text("schema_version = 1\n", encoding="utf-8")
            store = StateStore.initialize(home / "state.db")
            try:
                agent_id = store.create_agent(
                    StartRequest("claude", "model", "profile", "task", home,
                                 timeout_seconds=480),
                    task_summary="task",
                    config_revision="rev-1",
                ).agent_id
                store.append_event(
                    agent_id, "runtime_result", data=_RUNTIME_RESULT_PAYLOAD
                )
            finally:
                store.close()

            def run():
                stdout = io.StringIO()
                code = cli.main(
                    ["--home", str(home), "stats", "backfill"],
                    stdin=io.StringIO(),
                    stdout=stdout,
                    stderr=io.StringIO(),
                )
                return code, json.loads(stdout.getvalue())

            self.assertEqual(run(), (0, {"backfilled": 1, "skipped": 0}))
            self.assertEqual(run(), (0, {"backfilled": 0, "skipped": 0}))


if __name__ == "__main__":
    unittest.main()
