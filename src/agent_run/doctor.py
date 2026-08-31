"""Bounded, read-only installation diagnostics."""

from __future__ import annotations

import os
import re
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from .config import Config, RuntimeConfig, load_config
from .errors import AgentRunError, SchemaMigrationRequired
from .launch import launch_detached
from .launch_evidence import SupervisorBootstrapError
from .paths import config_path, state_db_path
from .state import diagnostic_snapshot

_LIMIT = 256
_SECRET = re.compile(
    r"(?i)^\s*[\w.-]*(?:secret|token|password|api[_-]?key)[\w.-]*\s*="
)
_MARKERS = {
    "codex": "config.toml",
    "claude": "settings.json",
    "opencode": "xdg/config/opencode/opencode.json",
    "qwen": ".qwen/settings.json",
    "glm": "settings.json",
}
#: Runtimes whose environment auth also has a macOS keychain item the adapter
#: falls back to when the variable is unset. Maps the config runtime name to
#: the ``(service, account)`` pair the item is stored under; an ``account`` of
#: ``None`` means the item has no fixed account, so it is probed by service
#: alone. Without this probe, doctor checks only ``os.environ`` and reports a
#: keychain-backed runtime as unauthenticated.
KEYCHAIN_FALLBACKS = {
    "qwen": ("com.pluto.agent-run.opencode.omniroute", "OMNIROUTE_API_KEY"),
    "glm": ("com.pluto.agent-run.glm", "GLM_CODING_KEY"),
    "claude": ("Claude Code-credentials", None),
}
#: Bound on the keychain probe so a hung ``security`` call cannot stall doctor.
_KEYCHAIN_TIMEOUT_SECONDS = 5.0


@dataclass(frozen=True, slots=True)
class DoctorFinding:
    code: str
    severity: str
    component: str
    detail: str


@dataclass(frozen=True, slots=True)
class DoctorReport:
    home: Path
    checked_at: float
    findings: tuple[DoctorFinding, ...]

    @property
    def ok(self) -> bool:
        return not any(item.severity == "error" for item in self.findings)


ProcessProbe = Callable[[int, int | None], tuple[bool, str | None, bool]]
#: Runs the provider-free canary handshake and returns its duration in
#: milliseconds, or raises (typically :class:`SupervisorBootstrapError`).
CanaryRunner = Callable[[], float]
#: Lists ``(pid, ps "lstart" text, ps "command" text)`` for every process on
#: the machine; swappable so inventory parsing is testable without a live ps.
McpProcessLister = Callable[[], list[tuple[int, str, str]]]


def run_doctor(
    home: str | Path,
    *,
    at: float | None = None,
    process_probe: ProcessProbe | None = None,
    canary_executable: str | None = None,
    canary_runner: CanaryRunner | None = None,
    mcp_process_lister: McpProcessLister | None = None,
) -> DoctorReport:
    root = Path(home).expanduser().resolve()
    checked_at = time.time() if at is None else float(at)
    findings: list[DoctorFinding] = []
    path = config_path(root)
    _plaintext_secrets(path, findings)
    try:
        config = load_config(path)
    except (OSError, AgentRunError) as error:
        _add(findings, "config_invalid", "error", "config", type(error).__name__)
        return DoctorReport(root, checked_at, tuple(findings))
    _configuration(config, path, root, findings)
    try:
        snapshot = diagnostic_snapshot(
            state_db_path(root), at=checked_at, limit=_LIMIT
        )
    except SchemaMigrationRequired as error:
        _add(
            findings,
            "state_migration_pending",
            "error",
            "state",
            f"schema v{error.found} awaits migration to v{error.expected}",
        )
        return DoctorReport(root, checked_at, tuple(findings))
    except (OSError, AgentRunError) as error:
        _add(findings, "state_invalid", "error", "state", type(error).__name__)
        return DoctorReport(root, checked_at, tuple(findings))
    _capacity(config, snapshot.capacity, checked_at, findings)
    _supervisors(snapshot.agents, process_probe or _probe_process, findings)
    _canary_handshake(
        findings, canary_runner or _bound_canary_runner(canary_executable)
    )
    _mcp_inventory(findings, root, mcp_process_lister or _list_mcp_processes)
    return DoctorReport(root, checked_at, tuple(findings[:_LIMIT]))


def _add(findings, code, severity, component, detail) -> None:
    if len(findings) < _LIMIT:
        findings.append(DoctorFinding(code, severity, component, detail))


def _plaintext_secrets(path: Path, findings: list[DoctorFinding]) -> None:
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return
    for number, line in enumerate(lines[:10_000], 1):
        if _SECRET.match(line):
            _add(
                findings,
                "plaintext_secret_config",
                "error",
                f"config:{number}",
                "secret-looking assignment",
            )


def _configuration(
    config: Config, path: Path, root: Path, findings: list[DoctorFinding]
) -> None:
    modified = path.stat().st_mtime
    for name, server in sorted(config.mcp.items())[:_LIMIT]:
        if not _executable(server.command):
            _add(findings, "mcp_executable_missing", "error", f"mcp:{name}", str(server.command))
    trusted = (root, (root / "standalone" / "current").resolve())
    for name, runtime in sorted(config.runtimes.items())[:_LIMIT]:
        if not runtime.enabled:
            continue
        component = f"runtime:{name}"
        if not _executable(runtime.binary):
            _add(findings, "runtime_binary_missing", "error", component, str(runtime.binary))
        if not runtime.home.is_dir():
            _add(findings, "runtime_home_missing", "error", component, str(runtime.home))
        else:
            marker = runtime.home / _MARKERS.get(name, "")
            if not marker.is_file():
                _add(findings, "runtime_home_unsupported", "error", component, str(marker))
            elif marker.stat().st_mtime < modified:
                _add(findings, "runtime_home_stale", "warning", component, str(marker))
        _skills(root, name, runtime, findings)
        _hooks(runtime, component, trusted, findings)
        _auth(name, runtime, component, findings)


def _skills(root: Path, name: str, runtime: RuntimeConfig, findings) -> None:
    for skill in runtime.skills:
        source = root / "skills" / name / skill / "SKILL.md"
        if not source.is_file():
            _add(findings, "runtime_skill_missing", "error", f"runtime:{name}", skill)


#: System interpreters our rendered hooks launch scripts with. Their own
#: absolute paths sit outside the trusted roots by design, so the trust check
#: for such a hook applies to the script argument instead.
_HOOK_INTERPRETERS = frozenset({"/usr/bin/python3", "/bin/sh", "/bin/zsh", "/usr/bin/env"})


def _hook_script(command: tuple[str, ...]) -> Path:
    """Return the path the hook trust check should evaluate.

    For a plain hook command that is ``command[0]``. When the hook launches a
    script through a known system interpreter (see :data:`_HOOK_INTERPRETERS`),
    the interpreter itself is not the artifact whose location matters, so the
    first non-flag argument -- the script path -- is returned. A hook that
    carries an interpreter but no such argument has nothing to trust, so the
    interpreter path is returned and the hook stays untrusted.

    ``command`` is the hook's argv; config parsing guarantees it is non-empty.
    """

    interpreter = Path(command[0]).expanduser()
    if str(interpreter) not in _HOOK_INTERPRETERS:
        return interpreter
    for word in command[1:]:
        if not word.startswith("-"):
            return Path(word).expanduser()
    return interpreter


def _hooks(runtime: RuntimeConfig, component: str, trusted, findings) -> None:
    """Check every runtime hook's presence and trust, recording violations.

    ``trusted`` are the roots a hook command must live under. The executable
    check always targets ``command[0]`` -- the interpreter, when there is one
    -- while the trust check targets :func:`_hook_script`, so a hook that runs
    a trusted script through a system interpreter is not flagged merely
    because the interpreter lives outside the roots.
    """

    for index, hook in enumerate(runtime.hooks):
        interpreter = Path(hook.command[0]).expanduser()
        item = f"{component}:hook:{index}"
        if not _executable(interpreter):
            _add(findings, "hook_executable_missing", "error", item, str(interpreter))
        script = _hook_script(hook.command)
        if not script.is_absolute() or not any(_under(script, root) for root in trusted):
            _add(findings, "hook_untrusted", "warning", item, str(script))


def _auth(name: str, runtime: RuntimeConfig, component: str, findings) -> None:
    """Check one runtime's auth wiring and record what is missing.

    ``name`` is the config runtime key (e.g. ``"qwen"``), used to look up a
    keychain fallback. Environment auth is satisfied by any of ``auth.names``
    being set, or by the runtime's fallback keychain item resolving; bridge
    auth is checked as a source file plus a symlink into the runtime home.
    Emits no finding when auth is configured and present.
    """

    auth = runtime.auth
    if auth is None:
        return
    if auth.kind == "environment":
        if not any(n in os.environ for n in auth.names) and not _keychain_present(name):
            _add(findings, "auth_environment_missing", "warning", component, ",".join(auth.names))
        return
    if auth.source is None or auth.target is None:
        _add(findings, "auth_bridge_metadata_missing", "error", component, auth.kind)
        return
    bridge = runtime.home / auth.target
    if not auth.source.exists():
        _add(findings, "auth_source_missing", "error", component, str(auth.source))
    if not bridge.is_symlink():
        _add(findings, "auth_bridge_missing", "error", component, str(bridge))
    elif bridge.resolve(strict=False) != auth.source.resolve(strict=False):
        _add(findings, "auth_bridge_mismatch", "error", component, str(bridge))


def _keychain_present(name: str) -> bool:
    """Whether the runtime's fallback keychain item exists and is readable.

    Probes ``security find-generic-password -s SERVICE -a ACCOUNT -w`` for the
    ``(service, account)`` pair registered in ``KEYCHAIN_FALLBACKS``; an entry
    registered with ``account=None`` is probed without ``-a``, because that
    item is stored with no fixed account. The secret is read only so that a
    resolvable item exits zero; the value is discarded immediately and never
    logged, returned, or stored. Any failure -- nonzero exit, missing binary,
    timeout, or ``OSError`` -- counts as "not present", so a broken probe never
    suppresses a real warning. Returns ``False`` for runtimes with no fallback
    entry.
    """

    fallback = KEYCHAIN_FALLBACKS.get(name)
    if fallback is None:
        return False
    service, account = fallback
    argv = ["security", "find-generic-password", "-s", service]
    if account is not None:
        argv += ["-a", account]
    argv.append("-w")
    try:
        result = subprocess.run(
            argv,
            capture_output=True,
            timeout=_KEYCHAIN_TIMEOUT_SECONDS,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired):
        return False
    return result.returncode == 0


def _capacity(config: Config, rows, at: float, findings) -> None:
    """Flag capacity identities whose newest sample no longer describes now.

    A sample that carries its own ``valid_until`` is judged only by it: a
    still-future ``valid_until`` is never stale (sources stamp their own
    validity window, which can be far longer than the collection interval),
    and a past one is always stale. Only a sample without ``valid_until``
    falls back to the age bound of twice the configured collection interval.
    ``rows`` are the snapshot's capacity samples; ``at`` is the check time in
    epoch seconds.
    """

    latest = {}
    for row in rows:
        key = tuple(row[name] for name in ("runtime", "lane", "window", "target", "source"))
        latest.setdefault(key, row)
    stale_after = max(1, config.capacity.collect_interval_seconds) * 2
    for key, row in latest.items():
        valid_until = row["valid_until"]
        if valid_until is not None:
            stale = float(valid_until) < at
        else:
            stale = at - float(row["observed_at"]) > stale_after
        if stale:
            _add(findings, "capacity_stale", "warning", f"capacity:{key[0]}", "/".join(str(x or "-") for x in key[1:]))


def _supervisors(rows, probe: ProcessProbe, findings) -> None:
    for row in rows:
        pid = row.get("supervisor_pid")
        pgid = row.get("process_group_id")
        expected = row.get("supervisor_identity")
        if not isinstance(pid, int):
            if row.get("status") in {"running", "cancelling"}:
                _add(findings, "dead_supervisor", "error", f"agent:{row['id']}", "missing pid")
            continue
        alive, identity, group_alive = probe(pid, pgid if isinstance(pgid, int) else None)
        if alive and identity is None:
            _add(findings, "supervisor_identity_unavailable", "warning", f"agent:{row['id']}", "process identity unavailable")
            continue
        dead = not alive or not isinstance(expected, str) or not (
            identity == expected or identity.endswith(f" {expected}")
        )
        if dead:
            _add(findings, "dead_supervisor", "error", f"agent:{row['id']}", "dead or identity mismatch")
            if group_alive:
                _add(findings, "suspected_orphan", "error", f"agent:{row['id']}", "engine group remains alive")


def _probe_process(pid: int, pgid: int | None) -> tuple[bool, str | None, bool]:
    alive = _exists(pid)
    identity = None
    if alive:
        try:
            result = subprocess.run(
                ["/bin/ps", "-p", str(pid), "-o", "command="],
                capture_output=True,
                text=True,
                timeout=1,
                env={"PATH": "/usr/bin:/bin"},
            )
            identity = result.stdout.strip() or None
        except (OSError, subprocess.TimeoutExpired):
            pass
    return alive, identity, False if pgid is None else _exists(pgid, group=True)


def _exists(value: int, *, group: bool = False) -> bool:
    try:
        (os.killpg if group else os.kill)(value, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _executable(path: Path) -> bool:
    return path.is_absolute() and path.is_file() and os.access(path, os.X_OK)


def _under(path: Path, root: Path) -> bool:
    try:
        path.resolve().relative_to(root.resolve())
    except (OSError, ValueError):
        return False
    return True


_CANARY_PS_ARGS_COMMAND = ["/bin/ps", "-A", "-o", "pid=,command="]
_CANARY_PS_ARGS_LSTART = ["/bin/ps", "-A", "-o", "pid=,lstart="]
_PS_ENV = {"PATH": "/usr/bin:/bin"}
_PS_TIMEOUT_SECONDS = 2
_LSTART_FORMAT = "%a %b %d %H:%M:%S %Y"
_LSTART_WHITESPACE = re.compile(r"\s+")


def _bound_canary_runner(executable: str | None) -> CanaryRunner:
    """Bind ``_run_canary`` to one executable so ``run_doctor`` can pass it around."""

    return lambda: _run_canary(executable or sys.executable)


def _run_canary(executable: str) -> float:
    """Run the real fork -> exec -> identity-proof -> READY handshake, provider-free.

    Targets a scratch temporary home and the real ``agent_run.supervisor_main``
    module via :func:`launch_detached`, exactly as ``start`` does, but with
    ``canary: True`` in the payload so the exec'd process proves identity,
    signals READY, and exits without ever touching an adapter or a session.
    Returns the handshake duration in milliseconds; raises
    :class:`SupervisorBootstrapError` (or another :class:`AgentRunError`) on
    any failure in that path, carrying the same bootstrap evidence a real
    failed ``start`` would.
    """

    started = time.monotonic()
    with tempfile.TemporaryDirectory(prefix="agent-run-doctor-canary-") as home:
        launch_detached({"home": home, "canary": True}, executable=executable)
    return (time.monotonic() - started) * 1000


def _canary_handshake(findings: list[DoctorFinding], runner: CanaryRunner) -> None:
    try:
        duration_ms = runner()
    except SupervisorBootstrapError as error:
        _add(findings, error.failure_kind, "error", "canary", _canary_detail(error))
    except AgentRunError as error:
        _add(findings, "supervisor_canary_failed", "error", "canary", type(error).__name__)
    else:
        _add(
            findings,
            "supervisor_canary_ok",
            "info",
            "canary",
            f"handshake completed in {duration_ms:.1f}ms",
        )


def _canary_detail(error: SupervisorBootstrapError) -> str:
    parts = [str(error)]
    if error.failure_stage:
        parts.append(f"stage={error.failure_stage}")
    if error.bootstrap_error_type:
        parts.append(f"type={error.bootstrap_error_type}")
    if error.provisional_pid is not None:
        parts.append(f"pid={error.provisional_pid}")
    return "; ".join(parts)


def _list_mcp_processes() -> list[tuple[int, str, str]]:
    """Pair every process's ``ps`` command with its start time.

    Two separate ``ps`` calls (rather than one combined format) because
    ``lstart`` embeds internal spaces that would make a single whitespace
    split ambiguous; each call instead needs only one split on the pid.
    """

    commands = _ps_by_pid(_CANARY_PS_ARGS_COMMAND)
    if not commands:
        return []
    starts = _ps_by_pid(_CANARY_PS_ARGS_LSTART)
    return [
        (pid, starts[pid], command)
        for pid, command in commands.items()
        if pid in starts
    ]


def _ps_by_pid(args: list[str]) -> dict[int, str]:
    try:
        result = subprocess.run(
            args, capture_output=True, text=True, timeout=_PS_TIMEOUT_SECONDS, env=_PS_ENV
        )
    except (OSError, subprocess.TimeoutExpired):
        return {}
    rows: dict[int, str] = {}
    for line in result.stdout.splitlines():
        parts = line.strip().split(None, 1)
        if len(parts) != 2:
            continue
        try:
            pid = int(parts[0])
        except ValueError:
            continue
        rows[pid] = parts[1]
    return rows


def _looks_like_mcp_process(tokens: list[str]) -> bool:
    if "mcp" not in tokens:
        return False
    return any(
        token == "agent_run.cli" or Path(token).name == "agent-run" for token in tokens
    )


def _parse_lstart(raw: str) -> float | None:
    normalized = _LSTART_WHITESPACE.sub(" ", raw.strip())
    try:
        return time.mktime(time.strptime(normalized, _LSTART_FORMAT))
    except ValueError:
        return None


def _mcp_inventory(
    findings: list[DoctorFinding], root: Path, lister: McpProcessLister
) -> None:
    """List every ``agent-run mcp`` process visible on the machine for this home.

    macOS has no ``/proc/<pid>/exe`` equivalent, so a running process's
    resolved release can never be read back from the OS once it has exec'd --
    only the argv path it was launched with (via ``ps``, e.g. through the
    ``standalone/current`` symlink), which may no longer match what that
    symlink points to today. This reports the honest available signal
    instead of a false-precision resolution: this process's own resolved
    release, the ``current`` symlink's target and last-switched time, and
    for every other mcp process, its pid, start time, and launch-path
    release hint -- flagged as a warning when the process started before
    the symlink's last switch, since it may still be running the code that
    was current at that time.
    """

    current_link = root / "standalone" / "current"
    try:
        switch_epoch = current_link.lstat().st_mtime
    except OSError:
        switch_epoch = None
    try:
        current_target = os.readlink(current_link)
    except OSError:
        current_target = None
    _add(
        findings,
        "mcp_inventory_self",
        "info",
        "mcp:self",
        f"release={os.path.realpath(sys.executable)} current_target={current_target or 'unknown'}",
    )
    self_pid = os.getpid()
    for pid, lstart_raw, command in lister()[:_LIMIT]:
        if pid == self_pid:
            continue
        tokens = command.split()
        if not _looks_like_mcp_process(tokens):
            continue
        release_hint = tokens[0]
        start_epoch = _parse_lstart(lstart_raw)
        started = (
            time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(start_epoch))
            if start_epoch is not None
            else lstart_raw.strip()
        )
        if (
            switch_epoch is not None
            and start_epoch is not None
            and start_epoch < switch_epoch
        ):
            _add(
                findings,
                "mcp_process_older_release",
                "warning",
                f"mcp:{pid}",
                f"started={started} release={release_hint}; started before the "
                "current release switch and may run older code -- reconnect "
                "MCP in this session before pruning releases",
            )
        else:
            _add(
                findings,
                "mcp_process",
                "info",
                f"mcp:{pid}",
                f"started={started} release={release_hint}",
            )

