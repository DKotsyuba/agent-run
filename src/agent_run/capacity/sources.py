"""Resolve which evidence source answers a runtime's capacity limits.

The collector used to ask every adapter for live limits; a runtime now names
its source explicitly (``native``, ``omniroute``, ``codexbar``, ``none``) so a
stale or never-refreshed native channel stops being the only answer. Every
source failure is no evidence, never an exception: ``None`` means the source
concept does not apply to this runtime, ``()`` means it applies and produced
no samples.
"""

from __future__ import annotations

import json
import logging
import math
import subprocess
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Mapping

from ..accounts import account_auth_source, account_email
from ..adapters import omniroute
from ..adapters.base import Capability, LimitSample, RuntimeAdapter
from ..adapters.claude.auth import keychain_token
from ..config import CapacityConfig, RuntimeConfig

_logger = logging.getLogger("agent_run.capacity")

Loader = Callable[[str, RuntimeConfig], RuntimeAdapter]

_CODEXBAR_PROVIDERS = {"codex": "codex", "claude": "claude", "glm": "zai"}
#: Extra codexbar arguments per RUNTIME. The claude provider's default "auto"
#: source reads browser cookies, which hangs past any timeout in the launchd
#: collector context (measured 30.08: 6 successful ticks out of 163 at 120s,
#: empty stderr) while the same call from an interactive shell finishes in
#: 11-54s. "--source cli" reads the local Claude CLI credentials instead:
#: deterministic, no cookie access, ~11s from any context.
_CODEXBAR_EXTRA_ARGS = {"claude": ("--source", "cli")}
_CODEXBAR_VALID_FOR_SECONDS = 900
#: 60s was not enough for the claude provider, which timed out on every tick
#: for hours while codex/glm answered in time -- the lane went blind on one
#: slow child. 120s keeps the tick well under the 900s validity window.
_CODEXBAR_TIMEOUT_SECONDS = 120
#: Bound on the stderr fragment carried into a failure log line, so a chatty
#: child cannot flood the log during a burst of failed ticks.
_ERROR_TAIL_CHARS = 200
_CODEXBAR_LANES = ("primary", "secondary", "tertiary")
_CLAUDE_NATIVE_URL = "https://api.anthropic.com/api/oauth/usage"
_CLAUDE_NATIVE_TIMEOUT_SECONDS = 20
_CLAUDE_NATIVE_VALID_FOR_SECONDS = 900


def _bounded_error_tail(stderr: object) -> str:
    """Collapse a child's stderr into one bounded line for failure logs.

    Accepts the ``stderr`` of a ``subprocess.CompletedProcess`` or of a
    ``subprocess.TimeoutExpired``, or ``None`` when the child never ran or
    produced nothing. Returns at most ``_ERROR_TAIL_CHARS`` characters with
    all whitespace flattened to single spaces, so the result never contains a
    newline; returns ``""`` when there is nothing to report.
    """

    if isinstance(stderr, bytes):
        stderr = stderr.decode("utf-8", "replace")
    if not isinstance(stderr, str) or not stderr:
        return ""
    return " ".join(stderr.split())[:_ERROR_TAIL_CHARS]


def _timestamp(value: object) -> datetime | None:
    """Parse one ISO-8601 stamp ('Z' suffix included); anything else is unknown."""

    if not isinstance(value, str) or len(value) > 64:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None else parsed


def _run_codexbar(argv: list[str]) -> subprocess.CompletedProcess:
    """One argv run with no shell; failures surface as no evidence upstream."""

    return subprocess.run(
        argv,
        capture_output=True,
        text=True,
        timeout=_CODEXBAR_TIMEOUT_SECONDS,
        check=False,
    )


def _codexbar_samples(
    name: str,
    capacity: CapacityConfig,
    runtime_config: RuntimeConfig,
    agent_run_home: Path | None,
) -> tuple[LimitSample, ...]:
    provider = _CODEXBAR_PROVIDERS.get(name)
    if provider is None:
        _logger.warning("codexbar source has no provider for runtime=%s", name)
        return ()
    argv = [str(capacity.codexbar_binary), "usage", "--provider", provider, "--json"]
    if runtime_config.accounts:
        argv.append("--all-accounts")
    argv += _CODEXBAR_EXTRA_ARGS.get(name, ())
    try:
        result = _run_codexbar(argv)
    except (OSError, subprocess.TimeoutExpired) as error:
        _logger.warning(
            "codexbar runtime=%s failed: %s stderr=%s",
            name,
            type(error).__name__,
            _bounded_error_tail(getattr(error, "stderr", None)),
        )
        return ()
    if result.returncode != 0:
        _logger.warning(
            "codexbar runtime=%s rc=%d stderr=%s",
            name,
            result.returncode,
            _bounded_error_tail(result.stderr),
        )
        return ()
    try:
        payload = json.loads(result.stdout)
    except ValueError:
        _logger.warning(
            "codexbar runtime=%s unparseable stdout stderr=%s",
            name,
            _bounded_error_tail(result.stderr),
        )
        return ()
    if isinstance(payload, Mapping):
        accounts = [payload]
    elif isinstance(payload, list):
        accounts = payload if runtime_config.accounts else payload[:1]
    else:
        accounts = []
    if not accounts:
        _logger.warning("codexbar runtime=%s returned no account entries", name)
        return ()

    declared_emails = {}
    if runtime_config.accounts and agent_run_home is not None:
        declared_emails = {
            label: account_email(account_auth_source(agent_run_home, name, label))
            for label in runtime_config.accounts
        }
    default_email = None
    if (
        runtime_config.auth is not None
        and runtime_config.auth.kind == "file_link"
        and runtime_config.auth.source is not None
    ):
        default_email = account_email(runtime_config.auth.source)

    samples = []
    for account in accounts:
        usage = account.get("usage") if isinstance(account, Mapping) else None
        if not isinstance(usage, Mapping):
            _logger.warning("codexbar runtime=%s missing usage object", name)
            continue
        identity = usage.get("identity")
        email = identity.get("accountEmail") if isinstance(identity, Mapping) else None
        if not isinstance(email, str) or not email:
            email = usage.get("accountEmail")
        if not isinstance(email, str) or not email:
            email = None
        if not runtime_config.accounts:
            target = None
        elif email is None:
            target = "unknown"
        elif email == default_email:
            target = None
        else:
            target = next(
                (label for label, declared_email in declared_emails.items() if declared_email == email),
                email,
            )

        observed_at = _timestamp(usage.get("updatedAt"))
        for lane in _CODEXBAR_LANES:
            window = usage.get(lane)
            if not isinstance(window, Mapping):
                continue
            used = window.get("usedPercent")
            if (
                isinstance(used, bool)
                or not isinstance(used, (int, float))
                or not math.isfinite(used)
            ):
                continue
            minutes = window.get("windowMinutes")
            label = (
                "five_hour"
                if minutes == 300
                else "seven_day"
                if minutes == 10080
                else f"min{minutes}"
            )
            samples.append(
                LimitSample(
                    lane=lane,
                    window=label,
                    remaining_percent=max(0.0, min(100.0, 100.0 - float(used))),
                    reset_at=_timestamp(window.get("resetsAt")),
                    observed_at=observed_at,
                    source="codexbar",
                    target=target,
                    valid_for_seconds=_CODEXBAR_VALID_FOR_SECONDS,
                )
            )
    if not samples:
        _logger.warning("codexbar runtime=%s returned no usable windows", name)
    return tuple(samples)


def _claude_native_samples() -> tuple[LimitSample, ...]:
    observed_at = time.time()
    token = keychain_token(observed_at)
    if token is None:
        _logger.warning("claude native limits have no usable OAuth token")
        return ()
    request = urllib.request.Request(
        _CLAUDE_NATIVE_URL,
        headers={
            "Authorization": f"Bearer {token}",
            "anthropic-beta": "oauth-2025-04-20",
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=_CLAUDE_NATIVE_TIMEOUT_SECONDS) as response:
            payload = json.loads(response.read())
    except (OSError, TimeoutError, ValueError) as error:
        _logger.warning(
            "claude native limits request failed: %s", type(error).__name__
        )
        return ()
    if not isinstance(payload, Mapping) or not isinstance(payload.get("limits"), list):
        _logger.warning("claude native limits response has no limits array")
        return ()

    observed = datetime.fromtimestamp(observed_at, tz=timezone.utc)
    samples = []
    for entry in payload["limits"]:
        if not isinstance(entry, Mapping):
            continue
        percent = entry.get("percent")
        if (
            isinstance(percent, bool)
            or not isinstance(percent, (int, float))
            or not math.isfinite(percent)
        ):
            continue
        kind = entry.get("kind")
        if kind == "session":
            lane, window, target = "primary", "five_hour", None
        elif kind == "weekly_all":
            lane, window, target = "secondary", "seven_day", None
        elif kind == "weekly_scoped":
            lane, window = "secondary", "seven_day"
            scope = entry.get("scope")
            model = scope.get("model") if isinstance(scope, Mapping) else None
            display_name = model.get("display_name") if isinstance(model, Mapping) else None
            target = display_name.lower() if isinstance(display_name, str) else str(kind)
        else:
            lane, window, target = "secondary", "seven_day", str(kind)
        samples.append(
            LimitSample(
                lane=lane,
                window=window,
                remaining_percent=max(0.0, min(100.0, 100.0 - float(percent))),
                reset_at=_timestamp(entry.get("resets_at")),
                observed_at=observed,
                source="native",
                target=target,
                valid_for_seconds=_CLAUDE_NATIVE_VALID_FOR_SECONDS,
            )
        )
    return tuple(samples)


def collect_samples(
    name: str,
    runtime_config: RuntimeConfig,
    capacity_config: CapacityConfig,
    load: Loader,
    agent_run_home: Path | None = None,
) -> tuple[LimitSample, ...] | None:
    """Resolve the runtime's configured source; ``None`` is unsupported."""

    source = runtime_config.limits_source or "native"
    if source == "none":
        return ()
    if source == "omniroute":
        return omniroute.pool_samples(time.time())
    if source == "codexbar":
        return _codexbar_samples(name, capacity_config, runtime_config, agent_run_home)
    if source == "native" and name == "claude":
        return _claude_native_samples()

    adapter = load(name, runtime_config)
    info = adapter.describe()
    if Capability.LIVE_LIMITS not in info.capabilities:
        return None
    return tuple(adapter.limits(runtime_config, runtime_config.home))
