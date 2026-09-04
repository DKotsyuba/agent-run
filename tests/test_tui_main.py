"""Tests for the dashboard entry point's loader wiring, without curses."""

from __future__ import annotations

import json
import socket
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch
import sys

sys.path.insert(0, str(Path(__file__).parents[1] / "src"))

from agent_run_tui.__main__ import default_loader, main
from agent_run_tui.model import Snapshot


#: One finished, unbound agent: the snapshot reader groups it without ever
#: resolving a host session title, so no runtime home is read.
AGENT = {"agent_id": "a1", "runtime": "codex", "model": None, "effort": None,
         "status": "succeeded", "started_at": 1.0, "finished_at": 2.0,
         "task_summary": "do work", "delivery": {"orchestrator_session_id": None}}
NULL_SESSION = {"session_id": None, "transport": "", "external_session_id": "",
                "active": 0, "total": 1, "last_seen_at": 2.0}


class SnapshotServer:
    """Answer dashboard requests for one client connection on a Unix socket.

    The serving thread is a daemon and ends when the client closes its
    connection or the test process exits; it holds no state between requests.
    """

    def __init__(self, path: Path) -> None:
        """Prepare an unstarted server bound to ``path`` once :meth:`start` runs."""
        self.path = path
        self.ready = threading.Event()
        self.thread = threading.Thread(target=self._serve, daemon=True)

    def start(self) -> None:
        """Start the server and wait until its socket accepts clients."""
        self.thread.start()
        self.ready.wait(2)

    def _serve(self) -> None:
        """Reply to every newline-delimited request on the first connection."""
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as server:
            server.bind(str(self.path))
            server.listen()
            self.ready.set()
            with server.accept()[0] as connection:
                for line in connection.makefile("rb"):
                    request = json.loads(line)
                    reply = {"jsonrpc": "2.0", "id": request["id"],
                             "result": self._result(request["method"], request["params"])}
                    connection.sendall(json.dumps(reply).encode() + b"\n")

    def _result(self, method: str, params: dict[str, object]) -> dict[str, object]:
        """Return the single-page result for one supported dashboard method.

        The one agent is finished, so it is absent from an ``active`` listing
        and present in an unfiltered one; the null orchestrator row counts it.
        """
        if method == "list_orchestrators":
            return {"items": [NULL_SESSION], "total": 1, "limit": params["limit"], "complete": True}
        if method == "list_agents":
            items = [] if params.get("active") else [AGENT]
            return {"items": items, "total": len(items), "offset": params["offset"],
                    "limit": params["limit"], "next_offset": None, "complete": True}
        raise AssertionError(method)


class MainTests(unittest.TestCase):
    """Check that command-line options select a working snapshot loader."""

    def loader_for(self, argv: list[str]):
        """Run ``main`` with curses stubbed out and return the dashboard's loader."""
        with patch("curses.wrapper") as wrapper:
            main(argv)
        return wrapper.call_args[0][0].__self__.loader

    def test_socket_option_builds_a_loader_that_reads_a_snapshot(self) -> None:
        """``--socket`` binds the real loader to that path; focusing a session reads its finished agents."""
        with tempfile.TemporaryDirectory(dir="/tmp") as directory:
            server = SnapshotServer(Path(directory) / "api.sock")
            server.start()
            loader = self.loader_for(["--socket", str(server.path)])
            self.assertTrue(callable(loader))
            snapshot = loader(100.0)
            self.assertIsInstance(snapshot, Snapshot)
            self.assertEqual(snapshot.observed_at, 100.0)
            self.assertEqual([card.session_id for card in snapshot.sessions], ["unbound"])
            self.assertEqual(snapshot.agents, {})
            loader.set_focus("unbound")
            self.assertEqual([card.agent_id for card in loader(101.0).agents["unbound"]], ["a1"])

    def test_default_loader_is_callable_without_contacting_the_socket(self) -> None:
        """Building the loader performs no I/O, so a missing socket is not an error."""
        with tempfile.TemporaryDirectory(dir="/tmp") as directory:
            self.assertTrue(callable(default_loader(Path(directory) / "absent.sock")))

    def test_demo_option_selects_the_built_in_snapshot(self) -> None:
        """``--demo`` uses the offline sample data instead of the API loader."""
        snapshot = self.loader_for(["--demo"])(100.0)
        self.assertIsInstance(snapshot, Snapshot)
        self.assertEqual([card.session_id for card in snapshot.sessions], ["ors_demo", "ors_review"])


if __name__ == "__main__":
    unittest.main()
