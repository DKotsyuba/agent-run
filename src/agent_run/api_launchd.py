"""Resident API daemon launchd plist generation."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
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
    return plistlib.dumps(
        {
            "Label": job.label,
            "ProgramArguments": list(argv(job)),
            "StandardOutPath": str(job.stdout_log),
            "StandardErrorPath": str(job.stderr_log),
            "RunAtLoad": True,
            "KeepAlive": True,
        },
        fmt=plistlib.FMT_XML,
        sort_keys=False,
    ).decode("utf-8")
