"""Resolve which evidence source answers a runtime's capacity limits.

The collector used to ask every adapter for live limits; a runtime now names
its source explicitly (``native``, ``omniroute``, ``codexbar``, ``none``) so a
stale or never-refreshed native channel stops being the only answer.
``None`` means the source concept does not apply to this runtime and ``()``
means it applied and legitimately observed no data; an operational failure
raises :class:`CapacitySourceError` with a fixed reason code instead of
masquerading as collected empty evidence. Failure logs carry static reason
codes, statuses, and exception class names only -- never provider output.
"""

from __future__ import annotations

import json
import logging
import math
import os
import ssl
import subprocess
import time
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Mapping

from ..accounts import account_auth_source, account_email
from ..adapters import omniroute
from ..adapters.base import Capability, LimitSample, RuntimeAdapter
from ..adapters.claude.auth import TOKEN_ENV_NAME, keychain_token, refresh_keychain
from ..config import CapacityConfig, RuntimeConfig
from ..errors import AuthError, CapacitySourceError
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
_CODEXBAR_LANES = ("primary", "secondary", "tertiary")
# Fixed failure reason codes. They are the only failure-dependent text that
# may reach a log line or a CollectResult: never raw provider output.
_CODEXBAR_SPAWN = "codexbar_spawn_failed"
_CODEXBAR_TIMEOUT = "codexbar_timeout"
_CODEXBAR_EXIT = "codexbar_nonzero_exit"
_CODEXBAR_MALFORMED = "codexbar_malformed_response"
_CODEXBAR_OBSERVED = "codexbar_invalid_observed_at"
_CODEXBAR_WINDOW = "codexbar_invalid_window"
_CODEXBAR_MISSING = "codexbar_missing_data"
_CLAUDE_TOKEN = "claude_token_missing"
_CLAUDE_REQUEST = "claude_usage_unreachable"
_CLAUDE_MALFORMED = "claude_malformed_response"
# Fixed codex app-server per-scope issue codes.
HOME_MISSING = "home_missing"
PROBE_FAILED = "probe_failed"
SCOPE_EMPTY = "scope_empty"
_CLAUDE_NATIVE_URL = "https://api.anthropic.com/api/oauth/usage"
_CLAUDE_NATIVE_TIMEOUT_SECONDS = 20
_CLAUDE_NATIVE_VALID_FOR_SECONDS = 900
#: Shelf life applied to a codex_appserver slice whose samples carry no
#: positive ``valid_for_seconds``; slices never stay fresh until the provider
#: reset, only for the bounded evidence interval.
_CODEX_APPSERVER_VALID_FOR_SECONDS = 900


def _finite_number(value: object) -> float | None:
    """Return ``value`` as a finite float, or ``None`` for anything else.

    Booleans are numbers in Python but never a provider quantity, so they
    are rejected like text, ``None``, and non-finite floats. Sources use
    this for ``usedPercent``/``windowMinutes``/``percent`` fields whose
    malformed presence must fail a round rather than silently disappear.
    """

    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    number = float(value)
    return number if math.isfinite(number) else None


def _timestamp(value: object) -> datetime | None:
    """Parse one timezone-aware ISO-8601 stamp; anything else is unknown.

    A naive stamp carries no usable timezone: interpreting it as UTC (or as
    local time) would invent freshness for evidence whose zone is unknown,
    so it is rejected exactly like an unparsable value.
    """

    if not isinstance(value, str) or len(value) > 64:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None else None


def _run_codexbar(argv: list[str]) -> subprocess.CompletedProcess:
    """One argv run with no shell; the caller classifies any failure."""

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
) -> tuple[LimitSample, ...] | None:
    """Collect one codexbar round; operational failures raise.

    ``name`` is the configured runtime, ``capacity`` supplies the codexbar
    binary, ``runtime_config`` names the configured account labels and the
    default auth link, and ``agent_run_home`` locates per-account auth
    files. A runtime with no codexbar provider is unsupported (``None``).
    Spawn, timeout, nonzero-exit, malformed-response, and missing-data
    failures raise :class:`CapacitySourceError` with a fixed reason code so
    no failed round can masquerade as collected empty evidence. A lane
    window that is present but unusable (non-numeric, boolean, or
    non-finite ``usedPercent``/``windowMinutes``, or a missing/naive
    ``updatedAt``) fails the whole round: silently dropping a governing
    window would make the route look unconstrained, and inventing a
    collector-now timestamp would revive stale evidence. Failure logs
    carry the static reason, exception type, and exit status only --
    never child output.
    """

    provider = _CODEXBAR_PROVIDERS.get(name)
    if provider is None:
        _logger.warning("codexbar source has no provider for runtime=%s", name)
        return None
    argv = [str(capacity.codexbar_binary), "usage", "--provider", provider, "--json"]
    if runtime_config.accounts:
        argv.append("--all-accounts")
    argv += _CODEXBAR_EXTRA_ARGS.get(name, ())
    try:
        result = _run_codexbar(argv)
    except subprocess.TimeoutExpired as error:
        _logger.warning(
            "codexbar runtime=%s reason=%s error=%s",
            name, _CODEXBAR_TIMEOUT, type(error).__name__,
        )
        raise CapacitySourceError(_CODEXBAR_TIMEOUT) from error
    except OSError as error:
        _logger.warning(
            "codexbar runtime=%s reason=%s error=%s",
            name, _CODEXBAR_SPAWN, type(error).__name__,
        )
        raise CapacitySourceError(_CODEXBAR_SPAWN) from error
    if result.returncode != 0:
        _logger.warning(
            "codexbar runtime=%s reason=%s rc=%d",
            name, _CODEXBAR_EXIT, result.returncode,
        )
        raise CapacitySourceError(_CODEXBAR_EXIT)
    try:
        payload = json.loads(result.stdout)
    except ValueError as error:
        _logger.warning(
            "codexbar runtime=%s reason=%s error=%s",
            name, _CODEXBAR_MALFORMED, type(error).__name__,
        )
        raise CapacitySourceError(_CODEXBAR_MALFORMED) from error
    if isinstance(payload, Mapping):
        accounts = [payload]
    elif isinstance(payload, list):
        accounts = payload if runtime_config.accounts else payload[:1]
    else:
        accounts = []
    if not accounts:
        _logger.warning("codexbar runtime=%s reason=%s", name, _CODEXBAR_MISSING)
        raise CapacitySourceError(_CODEXBAR_MISSING)

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
            _logger.warning("codexbar runtime=%s reason=%s", name, _CODEXBAR_MALFORMED)
            raise CapacitySourceError(_CODEXBAR_MALFORMED)
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
        if observed_at is None:
            _logger.warning("codexbar runtime=%s reason=%s", name, _CODEXBAR_OBSERVED)
            raise CapacitySourceError(_CODEXBAR_OBSERVED)
        for lane in _CODEXBAR_LANES:
            window = usage.get(lane)
            if not isinstance(window, Mapping):
                continue
            used = _finite_number(window.get("usedPercent"))
            minutes = _finite_number(window.get("windowMinutes"))
            if used is None or minutes is None or minutes <= 0:
                _logger.warning(
                    "codexbar runtime=%s reason=%s lane=%s",
                    name, _CODEXBAR_WINDOW, lane,
                )
                raise CapacitySourceError(_CODEXBAR_WINDOW)
            label = (
                "five_hour"
                if minutes == 300
                else "seven_day"
                if minutes == 10080
                else f"min{window.get('windowMinutes')}"
            )
            samples.append(
                LimitSample(
                    lane=lane,
                    window=label,
                    remaining_percent=max(0.0, min(100.0, 100.0 - used)),
                    reset_at=_timestamp(window.get("resetsAt")),
                    observed_at=observed_at,
                    source="codexbar",
                    target=target,
                    valid_for_seconds=_CODEXBAR_VALID_FOR_SECONDS,
                )
            )
    if not samples:
        _logger.warning("codexbar runtime=%s reason=%s", name, _CODEXBAR_MISSING)
        raise CapacitySourceError(_CODEXBAR_MISSING)
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


def _claude_oauth_token(runtime_config: RuntimeConfig, now: float) -> str | None:
    """Return the OAuth token for one native Claude collection round.

    An explicitly exported ``CLAUDE_CODE_OAUTH_TOKEN`` wins, but only when
    the runtime's auth configuration declares that variable: honouring an
    undeclared export would silently widen the configured auth bridge.
    ``ANTHROPIC_API_KEY`` is an API key and is never used as an OAuth
    token. Otherwise a usable Keychain token is returned directly. A missing
    or expired entry triggers the adapter-owned, 60-second-bounded Keychain
    refresh once, then this function rereads the Keychain. Refresh failures
    and still-unusable entries return ``None`` without exposing credential or
    child-process details. The caller maps that result to its fixed safe
    capacity-source outcome.
    """

    auth = runtime_config.auth
    if auth is not None and TOKEN_ENV_NAME in auth.names:
        exported = os.environ.get(TOKEN_ENV_NAME)
        if exported:
            return exported
    token = keychain_token(now)
    if token is not None:
        return token
    try:
        refresh_keychain(runtime_config.binary)
    except AuthError:
        return None
    return keychain_token(now)


def _claude_native_samples(
    runtime_config: RuntimeConfig,
) -> tuple[LimitSample, ...]:
    """Collect Claude's native usage round; operational failures raise.

    ``runtime_config`` supplies the auth declaration that gates an
    exported OAuth token. A missing OAuth token, an unreachable usage
    endpoint, and a malformed response each raise
    :class:`CapacitySourceError` with a fixed reason code -- none of them
    is collected empty evidence. A present ``limits`` entry whose
    ``percent`` is not a finite non-boolean number is also a failure:
    silently dropping it would make the model look unconstrained. A
    healthy empty ``limits`` array returns ``()``. Failure logs carry the
    static reason and exception type only.
    """

    observed_at = time.time()
    token = _claude_oauth_token(runtime_config, observed_at)
    if token is None:
        _logger.warning("claude native limits reason=%s", _CLAUDE_TOKEN)
        raise CapacitySourceError(_CLAUDE_TOKEN)
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
            body = response.read()
    except (OSError, TimeoutError) as error:
        _logger.warning(
            "claude native limits reason=%s error=%s",
            _CLAUDE_REQUEST, type(error).__name__,
        )
        raise CapacitySourceError(_CLAUDE_REQUEST) from error
    try:
        payload = json.loads(body)
    except ValueError as error:
        _logger.warning(
            "claude native limits reason=%s error=%s",
            _CLAUDE_MALFORMED, type(error).__name__,
        )
        raise CapacitySourceError(_CLAUDE_MALFORMED) from error
    if not isinstance(payload, Mapping) or not isinstance(payload.get("limits"), list):
        _logger.warning("claude native limits reason=%s", _CLAUDE_MALFORMED)
        raise CapacitySourceError(_CLAUDE_MALFORMED)

    observed = datetime.fromtimestamp(observed_at, tz=timezone.utc)
    samples = []
    for entry in payload["limits"]:
        percent = _finite_number(
            entry.get("percent") if isinstance(entry, Mapping) else None
        )
        if percent is None:
            _logger.warning("claude native limits reason=%s", _CLAUDE_MALFORMED)
            raise CapacitySourceError(_CLAUDE_MALFORMED)
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
                remaining_percent=max(0.0, min(100.0, 100.0 - percent)),
                reset_at=_timestamp(entry.get("resets_at")),
                observed_at=observed,
                source="native",
                target=target,
                valid_for_seconds=_CLAUDE_NATIVE_VALID_FOR_SECONDS,
            )
        )
    return tuple(samples)


@dataclass(frozen=True)
class CodexAppserverCollection:
    """One codex app-server collection round: healthy slices plus issues.

    ``slices`` holds every independently validated slice that may persist;
    ``issues`` holds fixed per-scope reason codes (:data:`HOME_MISSING`,
    :data:`PROBE_FAILED`, :data:`SCOPE_EMPTY`) for configured scopes that
    produced no slice this round. A duplicate backend account is a
    deliberate skip, never an issue. No backend account id ever appears
    in either field.
    """

    slices: tuple[CapacityCollectionSlice, ...]
    issues: tuple[str, ...] = ()


def collect_codex_appserver(
    name: str,
    runtime_config: RuntimeConfig,
    observed_at: float,
) -> CodexAppserverCollection:
    """Probe Codex base and configured account homes into independent slices.

    ``name`` is the configured runtime name, ``runtime_config`` supplies the
    Codex binary, base home, and ordered account labels, and ``observed_at`` is
    the collection epoch seconds shared by this round.  Each successful home
    is normalized and validated before it is returned as an opaque collection
    slice.  Every configured account is independent: a missing home, a failed
    probe, or an empty response records one fixed issue code and never
    removes another scope's slice, and subsequent accounts are still probed.
    A duplicate backend-account scope is a deliberate skip, not an issue.
    Backend account ids are retained only while de-duplicating this
    invocation and never appear in a slice, issue, or log; failure logs
    carry the issue code and exception class name only.
    """

    from ..accounts import account_runtime_home
    from ..adapters.codex.rate_limits import read_rate_limits
    from .codex_appserver import normalize_rate_limits

    homes = [(None, runtime_config.home)] + [
        (label, account_runtime_home(runtime_config.home, label))
        for label in runtime_config.accounts
    ]
    slices: list[CapacityCollectionSlice] = []
    issues: list[str] = []
    seen_account_ids: set[str] = set()
    for target, home in homes:
        scope = target if target is not None else "base"
        try:
            response = read_rate_limits(runtime_config, home)
            samples, topology, account_id = normalize_rate_limits(
                name, target, response, observed_at
            )
            if not samples or not getattr(topology, "routes", ()):
                issues.append(SCOPE_EMPTY)
                _logger.warning(
                    "codex_appserver runtime=%s target=%s issue=%s",
                    name, scope, SCOPE_EMPTY,
                )
                continue
            if account_id is not None and account_id in seen_account_ids:
                # Two configured labels resolving to one backend account is
                # deliberate de-duplication, not a collection issue.
                _logger.debug(
                    "codex_appserver runtime=%s target=%s duplicate_scope_skipped",
                    name, scope,
                )
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
        except FileNotFoundError as error:
            issue = HOME_MISSING if not home.is_dir() else PROBE_FAILED
            issues.append(issue)
            _logger.warning(
                "codex_appserver runtime=%s target=%s issue=%s error=%s",
                name, scope, issue, type(error).__name__,
            )
        except Exception as error:
            issues.append(PROBE_FAILED)
            _logger.warning(
                "codex_appserver runtime=%s target=%s issue=%s error=%s",
                name, scope, PROBE_FAILED, type(error).__name__,
            )
    return CodexAppserverCollection(tuple(slices), tuple(issues))


def collect_codex_appserver_slices(
    name: str,
    runtime_config: RuntimeConfig,
    observed_at: float,
) -> tuple[CapacityCollectionSlice, ...]:
    """Slice-only view of :func:`collect_codex_appserver` for legacy callers.

    Discards the per-scope issue diagnostics; new callers consume
    :func:`collect_codex_appserver` directly so a partially failed round is
    never mistaken for a fully collected one.
    """

    return collect_codex_appserver(name, runtime_config, observed_at).slices


def collect_samples(
    name: str,
    runtime_config: RuntimeConfig,
    capacity_config: CapacityConfig,
    load: Loader,
    agent_run_home: Path | None = None,
) -> tuple[LimitSample, ...] | None:
    """Resolve the runtime's configured source.

    ``None`` means the source concept does not apply to this runtime --
    including an explicit ``limits_source="none"`` and an adapter without
    live limits -- and ``()`` means the source applied and legitimately
    observed no data. Operational failures raise
    :class:`CapacitySourceError` with a fixed reason code rather than
    masquerading as empty evidence; adapter failures propagate unchanged
    for the collector to isolate per runtime.
    """

    source = runtime_config.limits_source or "native"
    if source == "none":
        return None
    if source == "omniroute":
        return omniroute.pool_samples(time.time())
    if source == "codexbar":
        return _codexbar_samples(name, capacity_config, runtime_config, agent_run_home)
    if source == "native" and name == "claude":
        return _claude_native_samples(runtime_config)

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
