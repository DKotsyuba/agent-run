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
import ssl
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
from .topology import (
    CapacityCollectionSlice,
    CapacityRouteDescriptor,
    CapacityTopology,
    PhysicalPoolDescriptor,
    account_token,
    pools_from_samples,
    validate_slice,
    validate_topology,
)

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
#: Shelf life applied to a codex_appserver slice whose samples carry no
#: positive ``valid_for_seconds``; slices never stay fresh until the provider
#: reset, only for the bounded evidence interval.
_CODEX_APPSERVER_VALID_FOR_SECONDS = 900


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


_SYSTEM_CA_BUNDLE = Path("/etc/ssl/cert.pem")


def _claude_native_context() -> ssl.SSLContext:
    """TLS context that verifies inside the sealed venv.

    The standalone venv's Python carries no CA bundle of its own, so the
    default context fails certificate verification on macOS; the system
    bundle at /etc/ssl/cert.pem is used when present.
    """

    if _SYSTEM_CA_BUNDLE.is_file():
        return ssl.create_default_context(cafile=str(_SYSTEM_CA_BUNDLE))
    return ssl.create_default_context()


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
        with urllib.request.urlopen(
            request,
            timeout=_CLAUDE_NATIVE_TIMEOUT_SECONDS,
            context=_claude_native_context(),
        ) as response:
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


def collect_codex_appserver_slices(
    name: str,
    runtime_config: RuntimeConfig,
    observed_at: float,
) -> tuple[CapacityCollectionSlice, ...]:
    """Probe Codex base and configured account homes into independent slices.

    ``name`` is the configured runtime name, ``runtime_config`` supplies the
    Codex binary, base home, and ordered account labels, and ``observed_at`` is
    the collection epoch seconds shared by this round.  Each successful home
    is normalized and validated before it is returned as an opaque collection
    slice.  One account failure is logged without response details and does
    not affect other homes; failed or zero-valid-sample scopes yield no slice,
    preserving any previously stored snapshot for natural aging.  Backend
    account ids are retained only while de-duplicating this invocation and are
    never included in a slice or log.
    """

    from ..accounts import account_runtime_home
    from ..adapters.codex.rate_limits import read_rate_limits
    from .codex_appserver import normalize_rate_limits

    homes = [(None, runtime_config.home)] + [
        (label, account_runtime_home(runtime_config.home, label))
        for label in runtime_config.accounts
    ]
    slices: list[CapacityCollectionSlice] = []
    seen_account_ids: set[str] = set()
    for target, home in homes:
        try:
            response = read_rate_limits(runtime_config, home)
            samples, topology, account_id = normalize_rate_limits(
                name, target, response, observed_at
            )
            if not samples or not getattr(topology, "routes", ()):
                continue
            if account_id is not None and account_id in seen_account_ids:
                continue
            if account_id is not None:
                seen_account_ids.add(account_id)
            # Evidence shelf life is the bounded source interval, never the
            # provider reset distance: a weekly reset must not keep a stale
            # topology snapshot fresh for days.
            valid_for = min(
                (
                    sample.valid_for_seconds
                    for sample in samples
                    if sample.valid_for_seconds and sample.valid_for_seconds > 0
                ),
                default=_CODEX_APPSERVER_VALID_FOR_SECONDS,
            )
            valid_until = observed_at + valid_for
            scope_id = f"codex:{account_token(target, absent_token='base')}"
            slices.append(validate_slice(
                name, scope_id, samples, topology, observed_at, valid_until
            ))
        except Exception as error:
            _logger.warning(
                "codex_appserver runtime=%s target=%s failed=%s",
                name, target if target is not None else "base", type(error).__name__,
            )
    return tuple(slices)


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


# --- source-specific topology normalizers -----------------------------------
#
# Everything below knows provider conventions; the topology module itself
# stays neutral. Normalizers turn already-collected LimitSample tuples into an
# explicit CapacityTopology: pools come from the neutral identity grouping,
# routes encode each source's launchability rules.


def _pool_index(
    pools: tuple[PhysicalPoolDescriptor, ...],
) -> dict[tuple[str, str, str | None, str], str]:
    """Map each pool's sample identity ``(lane, window, target, source)`` to its id.

    Each pool produced by :func:`pools_from_samples` holds exactly one
    identity, so the mapping is bijective for these pools; a pool with several
    keys would simply appear once per key.
    """

    return {
        (key.lane, key.window, key.target, key.source): pool.pool_id
        for pool in pools
        for key in pool.keys
    }


def _account_routes(
    name: str,
    samples: tuple[LimitSample, ...],
    launchable_targets: frozenset[str] | None,
) -> CapacityTopology:
    """Build one codexbar route per launchable account over all its pools.

    Pools are the neutral per-identity grouping; a route collects every pool
    for one account, where the account is the sample target (``None`` meaning
    the default account, which is always launchable). Its ``quota_lane`` is
    the routing-neutral ``default`` value: primary and secondary windows must
    never split an account into separate launch routes.
    ``launchable_targets`` is the set of configured account labels that may be
    launched; ``None`` means every target is launchable (the native
    scoped-model case). A discovered target outside ``launchable_targets``
    keeps its samples and pools but gets no route: it is evidence, never a
    launch target.
    """

    pools = pools_from_samples(name, samples)
    grouped: dict[str | None, set[str]] = {}
    for identity, pool_id in _pool_index(pools).items():
        target = identity[2]
        if (
            target is not None
            and launchable_targets is not None
            and target not in launchable_targets
        ):
            continue
        grouped.setdefault(target, set()).add(pool_id)
    routes = tuple(
        CapacityRouteDescriptor(
            route_id=f"{name}:{account_token(account, absent_token='default')}:default",
            runtime=name,
            account=account,
            quota_lane="default",
            pool_ids=tuple(sorted(pool_ids)),
        )
        for account, pool_ids in sorted(grouped.items(), key=lambda item: item[0] or "")
    )
    return validate_topology(pools, routes)


def _native_routes(name: str, samples: tuple[LimitSample, ...]) -> CapacityTopology:
    """Build Claude's shared route and scoped-model routes from sample targets.

    Target-less pools are shared by every model route and have one default
    route. Each non-empty target is a model scope, represented by a route with
    no account, a quota lane equal to that scope, and the union of shared and
    scope-specific pools. Names are data rather than a fixed model catalogue,
    so any configured or discovered scope follows the same rule.
    """

    pools = pools_from_samples(name, samples)
    shared: set[str] = set()
    scoped: dict[str, set[str]] = {}
    for identity, pool_id in _pool_index(pools).items():
        target = identity[2]
        if target is None:
            shared.add(pool_id)
        else:
            scoped.setdefault(target, set()).add(pool_id)
    routes: list[CapacityRouteDescriptor] = []
    if shared:
        routes.append(CapacityRouteDescriptor(
            route_id=f"{name}:default", runtime=name, account=None,
            quota_lane="default", pool_ids=tuple(sorted(shared)),
        ))
    for scope, pool_ids in sorted(scoped.items()):
        routes.append(CapacityRouteDescriptor(
            route_id=f"{name}:scope:{scope}", runtime=name, account=None,
            quota_lane=scope, pool_ids=tuple(sorted(shared | pool_ids)),
        ))
    return validate_topology(pools, routes)


def sample_topology(
    name: str,
    runtime_config: RuntimeConfig,
    samples: tuple[LimitSample, ...],
) -> CapacityTopology:
    """Derive the explicit topology one source's collected samples describe.

    ``codexbar`` restricts routes to the configured account labels (plus the
    default account), so discovered unknown accounts stay as pools and
    evidence without becoming launchable routes. The native path creates a
    shared default route plus one route per scope, each including shared
    capacity. ``omniroute`` reports one aggregate route over
    all of its pooled windows. The result is validated and canonically
    ordered; input sample order never changes it.
    """

    source = runtime_config.limits_source or "native"
    if source == "omniroute":
        pools = pools_from_samples(name, samples)
        return validate_topology(
            pools,
            (
                CapacityRouteDescriptor(
                    route_id=f"{name}:aggregate",
                    runtime=name,
                    account=None,
                    quota_lane="aggregate",
                    pool_ids=tuple(pool.pool_id for pool in pools),
                ),
            ),
        )
    if source == "codexbar":
        return _account_routes(name, samples, frozenset(runtime_config.accounts))
    return _native_routes(name, samples)
