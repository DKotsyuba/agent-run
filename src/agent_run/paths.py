"""Resolved paths below the private agent-run home."""

from __future__ import annotations

import os
from pathlib import Path

from .domain import AgentId, validate_agent_id
from .errors import PathEscapeError, ValidationError


def agent_run_home(value: str | Path | None = None) -> Path:
    raw = os.environ.get("AGENT_RUN_HOME", "~/.agent-run") if value is None else value
    if isinstance(raw, str) and not raw.strip():
        raise ValidationError("AGENT_RUN_HOME must not be blank")
    try:
        path = Path(raw).expanduser()
    except RuntimeError as error:
        raise ValidationError("AGENT_RUN_HOME contains an unresolved '~'") from error
    if str(path).startswith("~"):
        raise ValidationError("AGENT_RUN_HOME contains an unresolved '~'")
    return path.resolve()


def config_path(home: str | Path | None = None) -> Path:
    return agent_run_home(home) / "config.toml"


def state_db_path(home: str | Path | None = None) -> Path:
    return agent_run_home(home) / "state.db"


def _require_beneath(root: Path, candidate: Path) -> None:
    if not candidate.is_relative_to(root):
        raise PathEscapeError(f"path escapes agent-run home: {candidate}")


def agent_dir(agent_id: str | AgentId, home: str | Path | None = None) -> Path:
    root = agent_run_home(home)
    agents_root = (root / "agents").resolve()
    _require_beneath(root, agents_root)
    candidate = (agents_root / validate_agent_id(agent_id)).resolve()
    _require_beneath(agents_root, candidate)
    return candidate
