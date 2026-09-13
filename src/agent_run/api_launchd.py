"""Resident API daemon launchd plist generation."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import os
import plistlib

from .errors import ValidationError


API_SUBCOMMAND: tuple[str, ...] = ("api", "serve")


@dataclass(frozen=True, slots=True)
class ApiLaunchdJob:
    label: str
    binary: Path
    home: Path
    stdout_log: Path
    stderr_log: Path


def build_job(
    label: str,
    binary: Path,
    home: Path,
    *,
    stdout_log: Path,
    stderr_log: Path,
) -> ApiLaunchdJob:
    if not isinstance(label, str) or not label.strip():
        raise ValidationError("launchd label must be a nonblank string")
    for name, path in (
        ("binary", binary),
        ("home", home),
        ("stdout_log", stdout_log),
        ("stderr_log", stderr_log),
    ):
        if not isinstance(path, Path) or not path.is_absolute():
            raise ValidationError(f"{name} must be an absolute path")
    return ApiLaunchdJob(label, binary, home, stdout_log, stderr_log)


def argv(job: ApiLaunchdJob) -> tuple[str, ...]:
    return (str(job.binary), "--home", str(job.home)) + API_SUBCOMMAND


def render_plist(job: ApiLaunchdJob) -> str:
    """Render a keep-alive API LaunchAgent with headroom for runtime children.

    The generated job inherits the invoking user's home and optional ``PATH``.
    It also raises the per-process open-file soft limit to 65,536 so the broker,
    supervisors, and engine CLIs it launches do not inherit launchd's default
    limit of 256. The returned value is an XML plist string and no files are
    written.
    """

    # launchd gives jobs a bare PATH; engine CLIs the daemon launches (node
    # shims and friends) resolve helpers through PATH, so the generator bakes
    # the invoking shell's PATH into the job. Without this the first child
    # launched through a launchd daemon dies instantly (seen live).
    environment = {"HOME": str(Path.home())}
    path = os.environ.get("PATH")
    if path:
        environment["PATH"] = path
    return plistlib.dumps(
        {
            "Label": job.label,
            "ProgramArguments": list(argv(job)),
            "EnvironmentVariables": environment,
            "StandardOutPath": str(job.stdout_log),
            "StandardErrorPath": str(job.stderr_log),
            "SoftResourceLimits": {"NumberOfFiles": 65_536},
            "RunAtLoad": True,
            "KeepAlive": True,
        },
        fmt=plistlib.FMT_XML,
        sort_keys=False,
    ).decode("utf-8")
