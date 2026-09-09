"""One-shot launchd schedule for durable delivery retries."""

from __future__ import annotations

from dataclasses import dataclass
import math
from pathlib import Path
import plistlib

from ..config import DeliveryConfig
from ..errors import ValidationError


@dataclass(frozen=True)
class DeliveryLaunchdJob:
    label: str
    binary: Path
    home: Path
    interval_seconds: int
    stdout_log: Path
    stderr_log: Path


def build_configured_job(
    config: DeliveryConfig,
    label: str,
    binary: Path,
    home: Path,
    *,
    stdout_log: Path,
    stderr_log: Path,
) -> DeliveryLaunchdJob:
    if not isinstance(config, DeliveryConfig):
        raise ValidationError("config must be a DeliveryConfig")
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
    return DeliveryLaunchdJob(
        label,
        binary,
        home,
        max(1, math.ceil(config.retry_base_seconds)),
        stdout_log,
        stderr_log,
    )


def argv(job: DeliveryLaunchdJob) -> tuple[str, ...]:
    return (
        str(job.binary),
        "--home",
        str(job.home),
        "delivery",
        "dispatch",
    )


def render_plist(job: DeliveryLaunchdJob) -> str:
    return plistlib.dumps(
        {
            "Label": job.label,
            "ProgramArguments": list(argv(job)),
            "StartInterval": job.interval_seconds,
            "StandardOutPath": str(job.stdout_log),
            "StandardErrorPath": str(job.stderr_log),
            "RunAtLoad": False,
        },
        fmt=plistlib.FMT_XML,
        sort_keys=False,
    ).decode("utf-8")
