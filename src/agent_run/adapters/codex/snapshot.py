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

    Args:
        config (RuntimeConfig): Declared command refusals determine generated
            refusal wrappers; the configuration is not mutated.

    Returns:
        tuple[str, ...]: Paths relative to the generated runtime home. Skills
        and plugins remain bound through managed-tree manifests. No I/O occurs.
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

    Args:
        config (RuntimeConfig): Source of command refusals and the auth bridge.
        home (Path): Materialized runtime home whose index is rewritten.
        revision (str): Producer revision to bind, unchanged by this helper.

    Returns:
        None: The index is updated; authentication contents are never read.

    Raises:
        OSError: Auth-link resolution or snapshot I/O fails.
        ValidationError: The shared finalizer rejects a malformed artifact.

    Auth links are re-derived from ``config`` and their resolved targets are
    included in the index. Missing auth sources fail closed.
    """

    bridge = auth_bridge(config)
    managed_links: tuple[tuple[str, str], ...] = ()
    if bridge is not None:
        source, target = bridge
        managed_links = ((target, str(source.expanduser().resolve(strict=True))),)
    finalize_runtime_snapshots(home, revision, snapshot_files(config), managed_links)


def prepare_project_trust(home: Path, workdir: Path) -> bool:
    """Write Codex's exact trust receipt when ``workdir`` has none.

    Args:
        home (Path): Runtime home with a verified, regular TOML config file.
        workdir (Path): Already resolved launch directory receiving trust.

    Returns:
        bool: True only when the exact receipt was added to config.toml, so
        the caller can rebind its snapshot; False leaves existing bytes intact.

    Raises:
        ValidationError: The config is unreadable or malformed, its projects
            table is invalid, or an existing receipt is not exactly trusted.
        OSError: Writing the managed configuration fails.

    The caller must verify the snapshot before calling this fresh-launch-only
    helper; it does not authorize repairs of modified historical snapshots.
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
