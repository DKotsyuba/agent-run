"""launchd contract: one bounded capacity-collect command, no resident daemon.

launchd starts the job on ``StartInterval``, the process runs
``agent-run capacity collect --once`` to completion, and exits. There is no
``KeepAlive`` key, so launchd does not respawn a persistent process between
scheduled ticks.
"""

from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path
import plistlib

from ..config import CapacityConfig
from ..errors import ValidationError


COLLECT_SUBCOMMAND: tuple[str, ...] = ("capacity", "collect", "--once")


@dataclass(frozen=True)
class LaunchdJob:
    label: str
    binary: Path
    interval_seconds: int
    stdout_log: Path
    stderr_log: Path


def build_job(
    label: str,
    binary: Path,
    interval_seconds: int,
    *,
    stdout_log: Path,
    stderr_log: Path,
) -> LaunchdJob:
    if not isinstance(label, str) or not label.strip():
        raise ValidationError("launchd label must be a nonblank string")
    if (
        isinstance(interval_seconds, bool)
        or not isinstance(interval_seconds, int)
        or interval_seconds < 1
    ):
        raise ValidationError("interval_seconds must be an integer of at least 1")
    for name, path in (
        ("binary", binary),
        ("stdout_log", stdout_log),
        ("stderr_log", stderr_log),
    ):
        if not isinstance(path, Path) or not path.is_absolute():
            raise ValidationError(f"{name} must be an absolute path")
    return LaunchdJob(label, binary, interval_seconds, stdout_log, stderr_log)


def build_configured_job(
    config: CapacityConfig,
    label: str,
    binary: Path,
    *,
    stdout_log: Path,
    stderr_log: Path,
) -> LaunchdJob:
    if not isinstance(config, CapacityConfig):
        raise ValidationError("config must be a CapacityConfig")
    return build_job(
        label,
        binary,
        config.collect_interval_seconds,
        stdout_log=stdout_log,
        stderr_log=stderr_log,
    )


def argv(job: LaunchdJob) -> tuple[str, ...]:
    return (str(job.binary),) + COLLECT_SUBCOMMAND


def render_plist(job: LaunchdJob) -> str:
    """Render a one-shot collector with the invoking user's ``HOME`` and ``PATH``.

    launchd's default path omits common Node installation directories, while
    Codex app-server probes may invoke ``node`` through an env shebang. Only
    these two ordinary process-location variables are copied; credentials and
    all other ambient values stay outside the plist.
    """

    environment = {"HOME": str(Path.home())}
    if path := os.environ.get("PATH"):
        environment["PATH"] = path
    return plistlib.dumps(
        {
            "Label": job.label,
            "ProgramArguments": list(argv(job)),
            "EnvironmentVariables": environment,
            "StartInterval": job.interval_seconds,
            "StandardOutPath": str(job.stdout_log),
            "StandardErrorPath": str(job.stderr_log),
            "RunAtLoad": False,
        },
        fmt=plistlib.FMT_XML,
        sort_keys=False,
    ).decode("utf-8")
