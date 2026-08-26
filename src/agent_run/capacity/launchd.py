"""launchd contract: one bounded capacity-collect command, no resident daemon.

launchd starts the job on ``StartInterval``, the process runs
``agent-run capacity collect --once`` to completion, and exits. There is no
``KeepAlive`` key, so launchd does not respawn a persistent process between
scheduled ticks.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

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


def _escape(value: str) -> str:
    return value.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def render_plist(job: LaunchdJob) -> str:
    """Render a launchd property list for the bounded collector command."""

    program_arguments = "\n".join(
        f"        <string>{_escape(part)}</string>" for part in argv(job)
    )
    return (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" '
        '"http://www.apple.com/DTDs/PropertyList-1.0.dtd">\n'
        '<plist version="1.0">\n'
        "<dict>\n"
        "    <key>Label</key>\n"
        f"    <string>{_escape(job.label)}</string>\n"
        "    <key>ProgramArguments</key>\n"
        "    <array>\n"
        f"{program_arguments}\n"
        "    </array>\n"
        "    <key>StartInterval</key>\n"
        f"    <integer>{job.interval_seconds}</integer>\n"
        "    <key>StandardOutPath</key>\n"
        f"    <string>{_escape(str(job.stdout_log))}</string>\n"
        "    <key>StandardErrorPath</key>\n"
        f"    <string>{_escape(str(job.stderr_log))}</string>\n"
        "    <key>RunAtLoad</key>\n"
        "    <false/>\n"
        "</dict>\n"
        "</plist>\n"
    )
