"""Short-lived Codex app-server rate-limit probes owned by the adapter."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Mapping

from ...config import RuntimeConfig
from ..base import LaunchPlan
from . import app_server


_RATE_LIMIT_TIMEOUT_SECONDS = 20.0


def read_rate_limits(config: RuntimeConfig, home: Path) -> Mapping[str, object]:
    """Read rate limits for ``home`` through one adapter-owned app-server process.

    ``config`` supplies the Codex binary and ``home`` is either its base runtime
    home or an account-specific runtime home.  The launch environment confines
    Codex state to that home while retaining the invoking process's executable
    search path.  The raw response is returned for capacity normalization; no
    account identity or rate-limit payload is persisted by this helper.
    """

    plan = LaunchPlan(
        argv=(str(config.binary), "app-server"),
        cwd=Path(home),
        environment={
            "CODEX_HOME": str(home),
            "HOME": str(home),
            "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
        },
        initial_input=None,
        runtime_stream_path=Path(home) / ".rate-limits.jsonl",
        adapter_state={},
    )
    return app_server.fetch_rate_limits(plan, timeout_seconds=_RATE_LIMIT_TIMEOUT_SECONDS)
