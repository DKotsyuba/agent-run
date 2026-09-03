"""Short-lived Codex app-server rate-limit probes owned by the adapter."""

from __future__ import annotations

import time
from pathlib import Path
from typing import Mapping

from ...config import RuntimeConfig
from ...errors import ValidationError
from ..base import LaunchPlan
from . import app_server, environment


_RATE_LIMIT_TIMEOUT_SECONDS = 20.0


def read_rate_limits(config: RuntimeConfig, home: Path) -> Mapping[str, object]:
    """Read rate limits for ``home`` through one adapter-owned app-server process.

    ``config`` supplies the Codex binary and ``home`` is either its base runtime
    home or an account-specific runtime home.  The launch environment comes from
    :func:`environment.build_environment`, confining Codex state to that home
    while still resolving the binary's own packaged interpreter.  The raw
    response is returned for capacity normalization; no account identity or
    rate-limit payload is persisted by this helper.
    """

    plan = LaunchPlan(
        argv=(str(config.binary), "app-server"),
        cwd=Path(home),
        environment=environment.build_environment(config.binary, home),
        initial_input=None,
        runtime_stream_path=Path(home) / ".rate-limits.jsonl",
        adapter_state={},
    )
    return fetch_rate_limits(plan, timeout_seconds=_RATE_LIMIT_TIMEOUT_SECONDS)


def fetch_rate_limits(plan: LaunchPlan, *, timeout_seconds: float = 20.0) -> Mapping[str, object]:
    """Fetch one Codex account's raw rate-limit response within ``timeout_seconds``.

    ``plan`` is the short-lived app-server launch plan prepared by the Codex
    adapter.  The function initializes the server and reads
    ``account/rateLimits/read`` without opening an agent turn.  A positive,
    finite timeout bounds the entire exchange.  The returned response is a
    mapping exactly as supplied by the app-server; malformed responses raise
    :class:`ValidationError`.  The spawned transport is terminated and closed
    even when initialization, the request, or the deadline fails.
    """

    if (
        isinstance(timeout_seconds, bool)
        or not isinstance(timeout_seconds, (int, float))
        or not 0 < timeout_seconds < float("inf")
    ):
        raise ValidationError("timeout_seconds must be positive and finite")
    transport = app_server.ProcessTransport(plan)
    deadline = time.monotonic() + float(timeout_seconds)

    def remaining() -> float:
        """Return remaining exchange seconds or raise ``TimeoutError`` at deadline."""

        value = deadline - time.monotonic()
        if value <= 0:
            raise TimeoutError("codex app-server rate-limit refresh timed out")
        return value

    try:
        transport.request(
            "initialize",
            {"clientInfo": {"name": "agent-run", "version": "1"}},
            timeout_seconds=remaining(),
        )
        response = transport.request(
            "account/rateLimits/read", {}, timeout_seconds=remaining()
        )
        if not isinstance(response, Mapping):
            raise ValidationError("codex app-server rate-limit response must be a mapping")
        return response
    finally:
        try:
            transport.terminate(1.0)
        except Exception:
            transport.close()
