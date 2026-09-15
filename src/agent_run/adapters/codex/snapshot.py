"""Codex runtime snapshot and first-launch project-trust helpers."""

from __future__ import annotations

import tomllib
from pathlib import Path

from ...config import RuntimeConfig
from ...errors import ValidationError
from ..home import write_managed_file
from ..snapshots import finalize_runtime_snapshots
from .environment import auth_bridge
from .toml import toml_string


def snapshot_files(config: RuntimeConfig) -> tuple[str, ...]:
    """Return adapter-owned Codex files whose exact bytes bind a snapshot.

    ``config`` supplies declared command refusals, which determine generated
    refusal wrappers. Returned paths are relative to the generated runtime
    home; skills and plugins remain bound through managed-tree manifests.
    """

    denied_commands = config.environment.denied_commands if config.environment is not None else ()
    return (
        "config.toml",
        "rules/agent-run-command-policy.rules",
        "command-refusals/.agent-run-command-policy.json",
        *(f"command-refusals/{command}" for command in sorted(denied_commands)),
    )


def finalize_snapshots(config: RuntimeConfig, home: Path, revision: str) -> None:
    """Bind current Codex files and the declared auth bridge to ``revision``.

    Auth links are re-derived from ``config`` and their resolved targets are
    included in the index. Missing auth sources therefore fail closed through
    strict resolution in the shared snapshot finalizer.
    """

    bridge = auth_bridge(config)
    managed_links: tuple[tuple[str, str], ...] = ()
    if bridge is not None:
        source, target = bridge
        managed_links = ((target, str(source.expanduser().resolve(strict=True))),)
    finalize_runtime_snapshots(home, revision, snapshot_files(config), managed_links)


def prepare_project_trust(home: Path, workdir: Path) -> bool:
    """Write Codex's exact trust receipt when ``workdir`` has none.

    The generated config must be a valid regular TOML file and ``workdir``
    must already be resolved. Existing receipts must equal
    ``trust_level = "trusted"``; only newly written bytes return ``True`` so
    the caller can rebind the snapshot index.
    """

    config_path = home / "config.toml"
    try:
        payload = config_path.read_text(encoding="utf-8")
        document = tomllib.loads(payload)
    except (OSError, UnicodeDecodeError, tomllib.TOMLDecodeError) as error:
        raise ValidationError(f"codex generated config is unreadable: {error}") from error
    projects = document.get("projects")
    if projects is not None and not isinstance(projects, dict):
        raise ValidationError("codex generated config projects table is invalid")
    receipt = None if projects is None else projects.get(str(workdir))
    if receipt is not None:
        if receipt != {"trust_level": "trusted"}:
            raise ValidationError("codex generated config project trust receipt changed")
        return False
    write_managed_file(
        home,
        "config.toml",
        payload.rstrip() + f'\n\n[projects.{toml_string(str(workdir))}]\ntrust_level = "trusted"\n',
    )
    return True
