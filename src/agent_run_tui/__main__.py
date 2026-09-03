"""Command-line entry point for the standalone agent-run terminal dashboard."""

from __future__ import annotations

import argparse
import curses

from .app import Dashboard
from .model import AgentCard, SessionCard, Snapshot


def demo_snapshot(now: float) -> Snapshot:
    """Return a stable, representative snapshot for interactive dashboard demos."""
    sessions = (
        SessionCard("ors_demo", "stdio", "demo-1", "Implement dashboard", "/tmp/agent-run", 2, 3, now),
        SessionCard("ors_review", "socket", "demo-2", "Review release", "/tmp/release", 0, 1, now - 30),
    )
    agents = {
        "ors_demo": (
            AgentCard("agt_1", "codex", "gpt-5", "high", "running", True, "Build curses screens", now - 65, None, 65, "Writing render.py"),
            AgentCard("agt_2", "claude", None, None, "running", True, "Add boundary tests", now - 18, None, 18, "Running tests"),
            AgentCard("agt_3", "codex", "gpt-5", "low", "succeeded", False, "Design snapshot model", now - 80, now - 10, 70, None),
        ),
        "ors_review": (
            AgentCard("agt_4", "codex", None, None, "failed", False, "Inspect package metadata", now - 50, now - 20, 30, None, "timeout"),
        ),
    }
    return Snapshot(observed_at=now, sessions=sessions, agents=agents)


def default_loader(socket_path: object):
    """Build the real dashboard loader bound to ``socket_path`` once."""
    from pathlib import Path
    from .snapshot import make_loader
    return make_loader(Path(str(socket_path)))


def main(argv: list[str] | None = None) -> None:
    """Parse dashboard options and start curses with the selected snapshot loader."""
    parser = argparse.ArgumentParser(description="agent-run terminal dashboard")
    parser.add_argument("--refresh", type=float, default=2.0, help="refresh interval in seconds")
    from .api import default_socket_path
    parser.add_argument("--socket", default=default_socket_path(), help="resident API socket path")
    parser.add_argument("--demo", action="store_true", help="force built-in demo data")
    args = parser.parse_args(argv)
    loader = demo_snapshot if args.demo else default_loader(args.socket)
    curses.wrapper(Dashboard(loader, refresh_seconds=args.refresh).run)


if __name__ == "__main__":
    main()
