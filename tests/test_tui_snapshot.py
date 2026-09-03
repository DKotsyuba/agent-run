"""Tests for assembling dashboard snapshots from TUI API data."""

from __future__ import annotations

import unittest
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[1] / "src"))

from agent_run_tui.api import ApiError
from agent_run_tui.model import Snapshot
from agent_run_tui.snapshot import SnapshotState, load_snapshot, make_loader


SESSION = {"session_id": "s1", "transport": "codex_queue", "external_session_id": "thread",
           "active": 1, "total": 3, "last_seen_at": 10}
#: The service also reports a null-id aggregate row for unbound agents.
NULL_SESSION = {"session_id": None, "transport": "", "external_session_id": "",
                "active": 0, "total": 0, "last_seen_at": 9}


def agent(agent_id: str, session: str | None, **fields: object) -> dict[str, object]:
    """Build one API agent view delivered to ``session`` (``None`` means unbound).

    The defaults describe a finished agent started at 1.0 and finished at 2.0;
    ``fields`` overrides any of them, including ``delivery``.
    """
    item: dict[str, object] = {
        "agent_id": agent_id, "runtime": "codex", "model": None, "effort": None,
        "status": "succeeded", "started_at": 1.0, "finished_at": 2.0,
        "task_summary": agent_id, "delivery": {"orchestrator_session_id": session},
    }
    item.update(fields)
    return item


class FakeApi:
    """Mirror the resident API's paginated agent, session, and transcript replies.

    Pages honour the caller's ``offset``/``limit`` exactly as ``AgentPage`` does:
    ``next_offset`` is ``None`` on the final page.  Transcript pages validate the
    cursor like ``AgentService.transcript`` and answer ``next_cursor: None``
    whenever the page is complete, even when it carried messages.
    """

    def __init__(self, agents: list[dict[str, object]],
                 orchestrators: list[dict[str, object]] | None = None,
                 transcripts: dict[str, list[dict[str, object]]] | None = None) -> None:
        """Serve ``agents`` and ``orchestrators`` plus per-agent transcript messages."""
        self.agents = list(agents)
        self.orchestrators = list(orchestrators or [])
        self.transcripts = dict(transcripts or {})
        self.calls: list[tuple[str, dict[str, object]]] = []

    def params(self, method: str) -> list[dict[str, object]]:
        """Return, in order, every parameter mapping received for ``method``."""
        return [params for name, params in self.calls if name == method]

    def call(self, method: str, params: dict[str, object]) -> dict[str, object]:
        """Answer one recorded request, raising :class:`ApiError` on a bad cursor."""
        self.calls.append((method, params))
        if method == "list_orchestrators":
            return {"items": self.orchestrators, "total": len(self.orchestrators),
                    "limit": params["limit"], "complete": True}
        if method == "list_agents":
            offset, limit = params["offset"], params["limit"]
            page = self.agents[offset:offset + limit]
            consumed = offset + len(page)
            complete = consumed >= len(self.agents)
            return {"items": page, "total": len(self.agents), "offset": offset, "limit": limit,
                    "next_offset": None if complete else consumed, "complete": complete}
        if method == "transcript":
            cursor = params["cursor"]
            if isinstance(cursor, bool) or not isinstance(cursor, int) or cursor < 0:
                raise ApiError("cursor must be a nonnegative integer", -32602)
            pending = [message for message in self.transcripts.get(params["agent_id"], [])
                       if message["seq"] > cursor]
            page = pending[:params["limit"]]
            complete = len(page) == len(pending)
            return {"agent_id": params["agent_id"], "messages": page, "cursor": cursor,
                    "limit": params["limit"], "complete": complete,
                    "next_cursor": None if complete or not page else page[-1]["seq"]}
        raise AssertionError(method)


class Titles:
    """Provide deterministic metadata without reading a host runtime directory."""

    def resolve(self, transport: str, session: str) -> tuple[str, str | None]:
        """Return fixed title and cwd for the fake external session."""
        return ("title", "/cwd")


class SnapshotTests(unittest.TestCase):
    """Check the unfiltered sweep, local grouping, ordering, and cursor reuse."""

    def load(self, client: FakeApi, now: float = 100, **kw: object) -> Snapshot:
        """Load one snapshot from ``client`` with the deterministic title resolver."""
        return load_snapshot(now, client=client, titles=Titles(), **kw)

    def test_grouping_ordering_events_and_cursor(self) -> None:
        """One sweep groups by delivery session and only active agents page transcripts."""
        client = FakeApi(
            [agent("active", "s1", status="running", started_at=90, finished_at=None,
                   model="x", effort="high"),
             agent("new", "s1", finished_at=99),
             agent("old", "s1", status="failed", finished_at=98)],
            orchestrators=[SESSION, NULL_SESSION],
            transcripts={"active": [{"seq": 1, "at": 1, "role": "tool_call",
                                     "name": "Bash", "content": "x" * 90}]},
        )
        state = SnapshotState()
        snapshot = self.load(client, finished_limit=1, state=state)
        self.assertEqual([card.session_id for card in snapshot.sessions], ["s1"])
        self.assertEqual(sorted(snapshot.agents), ["s1"])
        cards = snapshot.agents["s1"]
        self.assertEqual([card.agent_id for card in cards], ["active", "new"])
        self.assertEqual((cards[0].elapsed_seconds, cards[0].last_event), (10, "Bash: " + "x" * 60))
        self.assertEqual(cards[1].elapsed_seconds, 98)
        self.assertEqual(client.params("list_agents"), [{"offset": 0, "limit": 200}])
        self.load(client, now=101, state=state)
        self.assertEqual([params["cursor"] for params in client.params("transcript")], [0, 1])

    def test_three_page_sweep_groups_every_page(self) -> None:
        """A 450-agent listing is followed through three pages and grouped locally."""
        agents = [agent(f"a{index}", "s1" if index % 2 else None) for index in range(450)]
        snapshot = self.load(client := FakeApi(agents, orchestrators=[SESSION]))
        self.assertEqual([params["offset"] for params in client.params("list_agents")],
                         [0, 200, 400])
        self.assertEqual(sorted(snapshot.agents), ["s1", "unbound"])
        counts = {card.session_id: (card.title, card.active, card.total)
                  for card in snapshot.sessions}
        self.assertEqual(counts["unbound"], ("unbound", 0, 225))
        self.assertEqual(len(snapshot.agents["unbound"]), 50)

    def test_sweep_stops_at_the_thousand_agent_cap(self) -> None:
        """An oversized listing stops after five pages and 1,000 collected agents."""
        client = FakeApi([agent(f"a{index}", None) for index in range(1200)])
        snapshot = self.load(client)
        self.assertEqual([params["offset"] for params in client.params("list_agents")],
                         [0, 200, 400, 600, 800])
        self.assertEqual([(card.session_id, card.total) for card in snapshot.sessions],
                         [("unbound", 1000)])

    def test_complete_page_resumes_from_the_last_sequence(self) -> None:
        """A complete page returns no cursor, so the next refresh sends the last seq."""
        messages = [{"seq": seq, "at": seq, "role": "assistant", "content": f"m{seq}"}
                    for seq in (4, 5)]
        client = FakeApi([agent("live", "s1", status="running", finished_at=None)],
                         orchestrators=[SESSION], transcripts={"live": messages})
        state = SnapshotState()
        self.load(client, state=state)
        self.assertEqual(state.cursors["live"], 5)
        second = self.load(client, now=101, state=state)
        self.assertEqual([params["cursor"] for params in client.params("transcript")], [0, 5])
        self.assertEqual(second.agents["s1"][0].last_event, "m5")
        with self.assertRaises(ApiError) as raised:
            client.call("transcript", {"agent_id": "live", "cursor": None, "limit": 200})
        self.assertEqual(raised.exception.code, -32602)

    def test_make_loader_uses_the_injected_client(self) -> None:
        """An injected client makes a reusable loader that yields snapshots when called."""
        client = FakeApi([agent("only", "s1")], orchestrators=[SESSION])
        loader = make_loader(client=client, titles=Titles(), state=SnapshotState())
        self.assertTrue(callable(loader))
        snapshot = loader(100.0)
        self.assertIsInstance(snapshot, Snapshot)
        self.assertEqual(snapshot.observed_at, 100.0)
        self.assertEqual([card.agent_id for card in snapshot.agents["s1"]], ["only"])


if __name__ == "__main__":
    unittest.main()
