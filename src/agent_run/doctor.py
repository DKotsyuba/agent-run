"""Bounded, read-only installation diagnostics."""

from __future__ import annotations

import os
import re
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from .config import Config, RuntimeConfig, load_config
from .errors import AgentRunError
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
}


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


def run_doctor(
    home: str | Path,
    *,
    at: float | None = None,
    process_probe: ProcessProbe | None = None,
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
    except (OSError, AgentRunError) as error:
        _add(findings, "state_invalid", "error", "state", type(error).__name__)
        return DoctorReport(root, checked_at, tuple(findings))
    _capacity(config, snapshot.capacity, checked_at, findings)
    _supervisors(snapshot.agents, process_probe or _probe_process, findings)
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
        _auth(runtime, component, findings)


def _skills(root: Path, name: str, runtime: RuntimeConfig, findings) -> None:
    for skill in runtime.skills:
        source = root / "skills" / name / skill / "SKILL.md"
        if not source.is_file():
            _add(findings, "runtime_skill_missing", "error", f"runtime:{name}", skill)


def _hooks(runtime: RuntimeConfig, component: str, trusted, findings) -> None:
    for index, hook in enumerate(runtime.hooks):
        command = Path(hook.command[0]).expanduser()
        item = f"{component}:hook:{index}"
        if not _executable(command):
            _add(findings, "hook_executable_missing", "error", item, str(command))
        if not command.is_absolute() or not any(_under(command, root) for root in trusted):
            _add(findings, "hook_untrusted", "warning", item, str(command))


def _auth(runtime: RuntimeConfig, component: str, findings) -> None:
    auth = runtime.auth
    if auth is None:
        return
    if auth.kind == "environment":
        if not any(name in os.environ for name in auth.names):
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


def _capacity(config: Config, rows, at: float, findings) -> None:
    latest = {}
    for row in rows:
        key = tuple(row[name] for name in ("runtime", "lane", "window", "target", "source"))
        latest.setdefault(key, row)
    stale_after = max(1, config.capacity.collect_interval_seconds) * 2
    for key, row in latest.items():
        valid_until = row["valid_until"]
        stale = (
            (valid_until is not None and float(valid_until) < at)
            or at - float(row["observed_at"]) > stale_after
        )
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
        dead = not alive or not isinstance(expected, str) or identity != expected
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
