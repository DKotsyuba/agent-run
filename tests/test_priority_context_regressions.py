"""Regression coverage for changed-only priority context and its CLI envelope.

Every case pins one observable contract of :func:`build_context` and the
``context_receipts`` persistence behind it: component-scoped deduplication,
budget clipping applied before fingerprinting, JSON-safe route identity
rendering, legacy receipt migration, session/transport isolation, concurrent
receipt writes, and the ``hookSpecificOutput`` CLI envelope.

The capacity ranking itself is replaced with deterministic stub orders via
``agent_run.capacity.order.build_capacity_order``; receipt persistence and
``build_context`` always run for real.
"""

import io
import json
import sys
import tempfile
import threading
import unittest
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from agent_run.cli import main
from agent_run.config import CapacityConfig, Config
from agent_run.domain import AgentStatus, OrchestratorRef, StartRequest
from agent_run.hooks.context import (
    ACTIVE_BLOCK_MAX_CHARS,
    CONTEXT_HARD_LIMIT_CHARS,
    _capacity_block,
    build_context,
)
from agent_run.state import StateStore

#: First words of the rendered priority block header.
PRIORITY_HEADER = "Runtime priorities (highest first)."

#: Omission instruction appended by ``_truncate_priority`` when clipping.
ORDER_HINT = "More routes omitted; use capacity_order if needed"

#: Active-block replacement used to force an active-only component change.
FORCED_ACTIVE_TEXT = "Active agents (1): alpha/model profile summary running 0m"
FORCED_ACTIVE_KEY = "1:ag-running:False:True"


def _alias(lane="lanes", account=None):
    """Return one route alias selector with ``lane`` and optional ``account``."""

    return SimpleNamespace(quota_lane=lane, account=account)


def _route(runtime, priority, aliases=None):
    """Return one stub ranked route rendering ``runtime`` at ``priority``."""

    return SimpleNamespace(
        runtime=runtime, priority=priority, aliases=tuple(aliases or (_alias(),))
    )


def _patch_order(routes, observed_at=1000.0):
    """Patch the shared order builder to return a deterministic stub order."""

    return patch(
        "agent_run.capacity.order.build_capacity_order",
        lambda _store, _config, *, now: SimpleNamespace(
            routes=tuple(routes), observed_at=observed_at
        ),
    )


def _patch_active(text, key):
    """Patch the active-agent renderer to return fixed text and semantic key."""

    return patch(
        "agent_run.hooks.context._active_block", lambda _agents, _at: (text, key)
    )


class ContextRegressionTests(unittest.TestCase):
    """Regression contracts for the context hook and its component receipts."""

    def setUp(self) -> None:
        """Create one temporary home, an initialized store, and one ref."""

        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name).resolve()
        self.store = StateStore.initialize(self.root / "state.db")
        self.config = Config(schema_version=1)
        self.ref = OrchestratorRef("codex_queue", "session-1", "turn-1")

    def tearDown(self) -> None:
        """Close the store and delete the temporary home."""

        self.store.close()
        self.temporary.cleanup()

    def receipt_row(self) -> dict:
        """Return the single context receipt row, failing when it is absent."""

        row = self.store.connection.execute(
            """SELECT orchestrator_session_id, context_key, injected_at
               FROM context_receipts"""
        ).fetchone()
        if row is None:
            self.fail("expected exactly one context receipt row")
        return dict(row)

    def receipt_key(self, session_id: str | None) -> str:
        """Return one session's stored receipt key, failing when it is absent."""

        if session_id is None:
            self.fail("a session id is required to read its receipt")
        row = self.store.connection.execute(
            "SELECT context_key FROM context_receipts WHERE orchestrator_session_id = ?",
            (session_id,),
        ).fetchone()
        if row is None:
            self.fail(f"no context receipt row for session {session_id}")
        return str(row["context_key"])

    def receipt_count(self) -> int:
        """Return how many context receipt rows exist."""

        return int(
            self.store.connection.execute(
                "SELECT COUNT(*) AS total FROM context_receipts"
            ).fetchone()["total"]
        )

    def bind_running_agent(self, *, at: float = 1.0) -> str:
        """Create one running agent bound to ``self.ref`` and return its id."""

        request = StartRequest(
            "codex", "model", "profile", "do work", self.root,
            timeout_seconds=480, orchestrator=self.ref,
        )
        agent_id = self.store.create_agent(
            request, task_summary="summary", config_revision="cfg-1", at=at
        ).agent_id
        self.store.bind_orchestrator(agent_id, self.ref, at=at)
        self.store.transition(agent_id, AgentStatus.STARTING, at=at)
        self.store.transition(agent_id, AgentStatus.RUNNING, at=at)
        return agent_id

    def test_active_appearance_keeps_the_same_priority_budget(self) -> None:
        """An empty-to-active transition cannot change the visible priority prefix."""
        from agent_run.hooks.context import _assemble

        text = "guidance\n" + "\n".join("x" * 100 for _ in range(24))
        self.assertEqual(_assemble(text, "", 1800)[0],
                         _assemble(text, "Active agents: one", 1800)[0])

    def test_malformed_component_payloads_are_legacy_not_crashes(self) -> None:
        """Malformed nested receipt payloads are rejected without attribute errors."""
        from agent_run.state.db import parse_context_components

        for value in ([], ["x"], "x", 1, True, {"p": ""}, {"": "hash"}):
            with self.subTest(value=value):
                self.assertIsNone(parse_context_components(json.dumps(
                    {"v": 2, "components": value})))

    def test_unicode_separators_cannot_split_a_route_line(self) -> None:
        """Escaped Unicode separators preserve one JSON-safe physical route line."""
        with _patch_order([_route("name\u2028line", 1.0)]):
            text = _capacity_block(self.store, self.config, 1000.0)
        self.assertNotIn("\u2028", text)
        self.assertIn("\\u2028", text)
        self.assertEqual(len(text.splitlines()), 2)

    def test_changed_only_priority_resends_after_returning_to_an_order(self) -> None:
        """A-B-A route orders deliver each change, then stay silent on noise."""

        alpha = [_route("alpha", 0.900)]
        beta = [_route("beta", 0.500)]
        with _patch_order(alpha):
            first = build_context(self.store, self.ref, config=self.config, now=1000.0)
            rendered = _capacity_block(self.store, self.config, 1000.0)
        self.assertTrue(first.injected)
        self.assertIn(f'1. runtime="alpha"', first.text)
        self.assertIn("priority=0.900", first.text)
        # Priority-only output: no active agents are claimed or listed.
        self.assertNotIn("Active agents", first.text)
        self.assertEqual(first.text, rendered)

        with _patch_order(beta):
            second = build_context(self.store, self.ref, config=self.config, now=1001.0)
        self.assertTrue(second.injected)
        self.assertIn('runtime="beta"', second.text)
        self.assertNotIn('runtime="alpha"', second.text)

        # Returning to the earlier order is a real change for the orchestrator.
        with _patch_order(alpha):
            third = build_context(self.store, self.ref, config=self.config, now=1002.0)
        self.assertTrue(third.injected)
        self.assertIn('runtime="alpha"', third.text)

        # Sub-3-decimal priority noise plus a new observed_at renders identically,
        # so the receipt is untouched and nothing is re-injected.
        with _patch_order([_route("alpha", 0.90041)], observed_at=9999.0):
            rendered = _capacity_block(self.store, self.config, 1003.0)
            silent = build_context(self.store, self.ref, config=self.config, now=1003.0)
        self.assertEqual(rendered, third.text)
        self.assertIn("priority=0.900", rendered)
        self.assertFalse(silent.injected)
        self.assertEqual(silent.text, "")
        self.assertEqual(silent.context_key, third.context_key)
        self.assertEqual(self.receipt_row()["injected_at"], 1002.0)

    def test_active_only_change_resends_the_active_block_without_priorities(self) -> None:
        """An active-only delta omits the priority header, even near the budget."""

        self.bind_running_agent()
        with _patch_order([_route("alpha", 0.900)]):
            first = build_context(self.store, self.ref, config=self.config, now=1000.0)
        self.assertTrue(first.injected)
        self.assertIn(PRIORITY_HEADER, first.text)
        self.assertIn("Active agents (1)", first.text)

        with _patch_active(FORCED_ACTIVE_TEXT, FORCED_ACTIVE_KEY):
            with _patch_order([_route("alpha", 0.900)]):
                second = build_context(
                    self.store, self.ref, config=self.config, now=1001.0
                )
        self.assertTrue(second.injected)
        self.assertIn(FORCED_ACTIVE_TEXT, second.text)
        self.assertNotIn(PRIORITY_HEADER, second.text)
        self.assertNotEqual(second.context_key, first.context_key)

        # With a near-budget allocation the active block consumes the whole
        # budget, so no priority slot exists at all and only "active" changes.
        tight = replace(
            self.config, capacity=CapacityConfig(context_max_chars=120)
        )
        with _patch_active(FORCED_ACTIVE_TEXT, FORCED_ACTIVE_KEY):
            with _patch_order([_route("alpha", 0.900)]):
                third = build_context(self.store, self.ref, config=tight, now=1002.0)
        self.assertTrue(third.injected)
        self.assertIn("Active agents", third.text)
        self.assertNotIn(PRIORITY_HEADER, third.text)
        self.assertLessEqual(len(third.text), 120)

    def test_zero_budget_writes_no_receipt_and_restored_budget_delivers(self) -> None:
        """A zero budget injects nothing and records neither session nor receipt."""

        self.bind_running_agent()
        bound_session = self.store.find_orchestrator_session(self.ref)
        unbound = OrchestratorRef("codex_queue", "never-bound", "turn-1")
        zero = replace(self.config, capacity=CapacityConfig(context_max_chars=0))
        with _patch_order([_route("alpha", 0.900)]):
            empty = build_context(self.store, unbound, config=zero, now=1000.0)
            bound_empty = build_context(self.store, self.ref, config=zero, now=1000.0)
        self.assertEqual((empty.text, empty.injected), ("", False))
        self.assertEqual((bound_empty.text, bound_empty.injected), ("", False))
        self.assertIsNone(empty.orchestrator_session_id)
        self.assertIsNone(self.store.find_orchestrator_session(unbound))
        self.assertEqual(
            bound_empty.orchestrator_session_id, bound_session
        )
        self.assertEqual(self.receipt_count(), 0)

        with _patch_order([_route("alpha", 0.900)]):
            restored = build_context(
                self.store, self.ref, config=self.config, now=1001.0
            )
        self.assertTrue(restored.injected)
        self.assertIn(PRIORITY_HEADER, restored.text)
        self.assertIn("Active agents (1)", restored.text)
        self.assertEqual(self.receipt_count(), 1)

    def test_tight_budget_clips_priority_and_growth_delivers_the_full_summary(self) -> None:
        """Clipping hides later route lines, so their changes stay silent."""

        routes = [_route("alpha", 0.900), _route("beta", 0.500), _route("gamma", 0.100)]
        with _patch_order(routes):
            full = _capacity_block(self.store, self.config, 1000.0)
        self.assertNotIn(ORDER_HINT, full)
        # The active slot is reserved even when no agent is active, so a budget
        # at or below the reservation leaves no priority slot at all.
        reserved = replace(
            self.config,
            capacity=CapacityConfig(context_max_chars=ACTIVE_BLOCK_MAX_CHARS),
        )
        with _patch_order(routes):
            unslotted = build_context(self.store, self.ref, config=reserved, now=1000.0)
        self.assertEqual((unslotted.text, unslotted.injected), ("", False))

        # Keep exactly the header, the first route line, and the omission hint.
        prefix = "\n".join(full.splitlines()[:2])
        tight_budget = (
            ACTIVE_BLOCK_MAX_CHARS + 1 + len(prefix) + 1 + len(ORDER_HINT)
        )
        tight = replace(
            self.config, capacity=CapacityConfig(context_max_chars=tight_budget)
        )

        with _patch_order(routes):
            clipped = build_context(self.store, self.ref, config=tight, now=1000.0)
        self.assertTrue(clipped.injected)
        self.assertIn(ORDER_HINT, clipped.text)
        self.assertIn('runtime="alpha"', clipped.text)
        self.assertNotIn('runtime="beta"', clipped.text)
        self.assertLessEqual(len(clipped.text), tight_budget)

        # A change on a clipped-away line cannot be seen by the orchestrator and
        # must not resend the same visible prefix.
        hidden = [routes[0], _route("beta", 0.444), routes[2]]
        with _patch_order(hidden):
            silent = build_context(self.store, self.ref, config=tight, now=1001.0)
        self.assertFalse(silent.injected)
        self.assertEqual(silent.text, "")

        # Growing the budget reveals the previously hidden line and delivers it.
        grown = replace(
            self.config,
            capacity=CapacityConfig(context_max_chars=CONTEXT_HARD_LIMIT_CHARS),
        )
        with _patch_order(hidden):
            delivered = build_context(
                self.store, self.ref, config=grown, now=1002.0
            )
        self.assertTrue(delivered.injected)
        self.assertNotIn(ORDER_HINT, delivered.text)
        self.assertIn('runtime="beta"', delivered.text)
        self.assertIn("priority=0.444", delivered.text)

    def test_route_identity_is_json_safe_and_aliases_collapse_per_route(self) -> None:
        """Control characters cannot forge lines; literal and absent accounts differ."""

        injected_runtime = "codex\n1. runtime=fake"
        routes = [
            _route("alpha", 0.800, [_alias(account="none")]),
            _route(
                injected_runtime,
                0.900,
                [_alias("lane-a", None), _alias("lane-a", "none")],
            ),
        ]
        with _patch_order(routes):
            text = _capacity_block(self.store, self.config, 1000.0)
        lines = text.splitlines()
        self.assertEqual(len(lines), 3)
        self.assertEqual([line[:2] for line in lines[1:]], ["1.", "2."])
        for line in lines[1:]:
            self.assertEqual(line.count("selectors="), 1)

        # Every alias of one physical route renders on that route's single line.
        selectors = [
            json.loads(line.split("selectors=", 1)[1].rsplit("; priority=", 1)[0])
            for line in lines[1:]
        ]
        self.assertEqual(selectors[0], [{"quota_lane": "lanes", "account": "none"}])
        self.assertEqual(
            selectors[1],
            [{"quota_lane": "lane-a"}, {"quota_lane": "lane-a", "account": "none"}],
        )

        # A literal "none" account stays an explicit selector; an absent account
        # omits the key entirely, matching the header's account=null meaning.
        self.assertIn(
            f"runtime={json.dumps(injected_runtime, ensure_ascii=False)}", lines[2]
        )
        self.assertFalse(
            any(line.startswith("1. runtime=fake") for line in lines),
            "a control character must not start a new priority line",
        )
        self.assertTrue(lines[1].endswith("priority=0.800"))

        with _patch_order(routes):
            delivered = build_context(
                self.store, self.ref, config=self.config, now=1000.0
            )
        self.assertTrue(delivered.injected)
        self.assertIn('"account":"none"', delivered.text)
        self.assertEqual(len(delivered.text.splitlines()), 3)

    def test_legacy_digest_receipt_is_replaced_in_place_and_components_preserved(
        self,
    ) -> None:
        """A legacy plain-digest row migrates in place; untouched parts survive."""

        self.bind_running_agent()
        with _patch_order([_route("alpha", 0.900)]):
            first = build_context(self.store, self.ref, config=self.config, now=1000.0)
        self.assertTrue(first.injected)
        session_id = self.store.find_orchestrator_session(self.ref)
        self.assertEqual(self.receipt_count(), 1)

        # Downgrade the row to the pre-component single-digest encoding.
        legacy = "a" * 64
        self.store.connection.execute(
            """UPDATE context_receipts SET context_key = ?, injected_at = ?
               WHERE orchestrator_session_id = ?""",
            (legacy, 500.0, session_id),
        )
        self.store.connection.commit()

        with _patch_order([_route("alpha", 0.900)]):
            second = build_context(
                self.store, self.ref, config=self.config, now=1001.0
            )
        self.assertTrue(second.injected)
        row = self.receipt_row()
        self.assertEqual(row["orchestrator_session_id"], session_id)
        self.assertEqual(row["injected_at"], 1001.0)
        self.assertNotIn(legacy, row["context_key"])
        migrated = json.loads(row["context_key"])
        self.assertEqual(migrated["v"], 2)
        self.assertEqual(sorted(migrated["components"]), ["active", "priority"])

        # An unchanged delivery rewrites nothing: the timestamp stays put.
        with _patch_order([_route("alpha", 0.900)]):
            third = build_context(self.store, self.ref, config=self.config, now=9999.0)
        self.assertFalse(third.injected)
        self.assertEqual(self.receipt_row()["injected_at"], 1001.0)

        # Dropping the active component leaves its stored fingerprint intact and
        # delivers only the priority line that actually changed.
        with _patch_active("", "0:0"):
            with _patch_order([_route("alpha", 0.901)]):
                fourth = build_context(
                    self.store, self.ref, config=self.config, now=1002.0
                )
        self.assertTrue(fourth.injected)
        self.assertIn("priority=0.901", fourth.text)
        self.assertNotIn("Active agents", fourth.text)
        stored = json.loads(self.receipt_row()["context_key"])["components"]
        self.assertEqual(sorted(stored), ["active", "priority"])
        self.assertNotEqual(stored["priority"], migrated["components"]["priority"])
        self.assertEqual(stored["active"], migrated["components"]["active"])

    def test_sessions_and_transports_keep_independent_receipts(self) -> None:
        """Separate refs never share a session, a receipt row, or a silence."""

        self.bind_running_agent()
        other_transport = OrchestratorRef("claude_uds", "session-1", "turn-1")
        other_session = OrchestratorRef("codex_queue", "session-2", "turn-1")
        with _patch_order([_route("alpha", 0.900)]):
            mine = build_context(self.store, self.ref, config=self.config, now=1000.0)
            via_other_transport = build_context(
                self.store, other_transport, config=self.config, now=1000.5
            )
            other = build_context(
                self.store, other_session, config=self.config, now=1001.0
            )
        self.assertTrue(mine.injected)
        self.assertTrue(via_other_transport.injected)
        self.assertTrue(other.injected)
        self.assertEqual(
            len(
                {
                    mine.orchestrator_session_id,
                    via_other_transport.orchestrator_session_id,
                    other.orchestrator_session_id,
                }
            ),
            3,
        )
        self.assertEqual(self.receipt_count(), 3)

        with _patch_order([_route("alpha", 0.900)]):
            for ref in (self.ref, other_transport, other_session):
                repeat = build_context(self.store, ref, config=self.config, now=1002.0)
                self.assertFalse(repeat.injected)
                self.assertEqual(repeat.text, "")

        # Each receipt is independent: delivering to one ref leaves the other
        # refs' stored fingerprints alone until they are asked themselves.
        transport_key = self.receipt_key(via_other_transport.orchestrator_session_id)
        other_key = self.receipt_key(other.orchestrator_session_id)
        with _patch_order([_route("alpha", 0.100)]):
            changed = build_context(
                self.store, self.ref, config=self.config, now=1003.0
            )
            self.assertEqual(
                self.receipt_key(other.orchestrator_session_id), other_key
            )
            other_changed = build_context(
                self.store, other_session, config=self.config, now=1003.5
            )
        self.assertTrue(changed.injected)
        self.assertIn("priority=0.100", changed.text)
        self.assertTrue(other_changed.injected)
        self.assertIn("priority=0.100", other_changed.text)
        self.assertNotEqual(
            self.receipt_key(other.orchestrator_session_id), other_key
        )
        # The transport-scoped ref was never asked, so it still holds the old order.
        self.assertEqual(
            self.receipt_key(via_other_transport.orchestrator_session_id), transport_key
        )

    def test_concurrent_identical_component_receipts_change_exactly_once(self) -> None:
        """Racing identical receipt writes report one winner and the same session."""

        with _patch_order([_route("alpha", 0.900)]):
            first = build_context(self.store, self.ref, config=self.config, now=1000.0)
        session_id = first.orchestrator_session_id
        components = {"priority": "p-1", "active": "a-1"}
        database = self.store.path()
        workers = 4
        barrier = threading.Barrier(workers)
        outcomes: list[tuple[str, frozenset[str]]] = []
        errors: list[BaseException] = []

        def worker() -> None:
            """Record the same components from this worker's own connection."""

            try:
                store = StateStore.open(database)
                try:
                    barrier.wait(timeout=10)
                    outcomes.append(
                        store.record_context_components_for_ref(
                            self.ref, components, at=1500.0
                        )
                    )
                finally:
                    store.close()
            except BaseException as error:  # surfaced by the assertions below
                errors.append(error)

        threads = [
            threading.Thread(target=worker) for _index in range(workers)
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=30)
        self.assertEqual(errors, [])
        self.assertEqual(len(outcomes), workers)
        self.assertEqual({session_id for session_id, _names in outcomes}, {session_id})
        self.assertEqual(
            sorted(sorted(names) for _session, names in outcomes),
            [[], [], [], ["active", "priority"]],
        )
        self.assertEqual(self.receipt_count(), 1)
        self.assertEqual(
            json.loads(self.receipt_row()["context_key"])["components"], components
        )
        self.assertEqual(self.receipt_row()["injected_at"], 1500.0)


class HookContextCliTests(unittest.TestCase):
    """Drive the real ``hook context`` command end to end against a temp home."""

    def setUp(self) -> None:
        """Write a minimal config and initialize the temporary home's database."""

        self.temporary = tempfile.TemporaryDirectory()
        self.home = Path(self.temporary.name).resolve()
        (self.home / "config.toml").write_text("schema_version = 1\n", encoding="utf-8")
        StateStore.initialize(self.home / "state.db").close()

    def tearDown(self) -> None:
        """Delete the temporary home."""

        self.temporary.cleanup()

    def hook(self) -> tuple[int, dict, str]:
        """Run one real ``hook context`` invocation and return code and payload."""

        stdout, stderr = io.StringIO(), io.StringIO()
        payload = json.dumps(
            {
                "session_id": "external-cli-1",
                "hook_event_name": "UserPromptSubmit",
                "prompt": "continue",
            }
        )
        code = main(
            ["--home", str(self.home), "hook", "context"],
            stdin=io.StringIO(payload),
            stdout=stdout,
            stderr=stderr,
        )
        return code, json.loads(stdout.getvalue()), stderr.getvalue()

    def test_hook_context_delivers_changes_then_an_empty_payload(self) -> None:
        """The first call injects, the second is empty, and a change re-injects."""

        with _patch_order([_route("alpha", 0.900)]):
            code, first, _stderr = self.hook()
        self.assertEqual(code, 0)
        self.assertEqual(set(first), {"hookSpecificOutput"})
        envelope = first["hookSpecificOutput"]
        self.assertEqual(set(envelope), {"hookEventName", "additionalContext"})
        self.assertEqual(envelope["hookEventName"], "UserPromptSubmit")
        self.assertIn(PRIORITY_HEADER, envelope["additionalContext"])
        self.assertIn('runtime="alpha"', envelope["additionalContext"])
        self.assertIn("priority=0.900", envelope["additionalContext"])

        with _patch_order([_route("alpha", 0.900)]):
            code, second, _stderr = self.hook()
        self.assertEqual(code, 0)
        self.assertEqual(second, {})

        with _patch_order([_route("beta", 0.500)]):
            code, third, _stderr = self.hook()
        self.assertEqual(code, 0)
        envelope = third["hookSpecificOutput"]
        self.assertEqual(envelope["hookEventName"], "UserPromptSubmit")
        self.assertIn('runtime="beta"', envelope["additionalContext"])
        self.assertNotIn('runtime="alpha"', envelope["additionalContext"])
        self.assertIn("priority=0.500", envelope["additionalContext"])


if __name__ == "__main__":  # pragma: no cover - manual convenience only
    unittest.main()
