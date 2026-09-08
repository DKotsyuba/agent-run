"""Shared child-process environment for adapter-owned Codex launches.

Every adapter-owned Codex subprocess -- agent threads, model-roster refreshes,
and rate-limit probes -- runs with a fully replaced environment: Codex state
confined to one generated home, and an executable search path able to resolve
the packaged launcher's own interpreter.  The launchd collector has no
interactive shell, so its inherited ``PATH`` cannot resolve a
``#!/usr/bin/env node`` style interpreter that ships in the same package
directory as the configured binary, which is why that directory is prefixed
here rather than taken from the invoking process alone.
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from pathlib import Path

from ...config import McpConfig, RuntimeConfig
from ...errors import ValidationError
from ..command_policy import materialize_refusal_commands, render_codex_denial_rules
from ..developer_environment import developer_environment
from ..home import managed_uv_python_environment, write_managed_file


def resolved_directory(value: object, label: str) -> Path:
    """Return an existing directory resolved from ``value``.

    ``label`` identifies the configuration field in the typed validation error.
    Invalid path values, resolution failures, and non-directory targets raise
    ``ValidationError`` without changing filesystem state.
    """

    try:
        resolved = Path(value).expanduser().resolve(strict=True)
    except (TypeError, OSError, RuntimeError) as error:
        raise ValidationError(f"{label} must be an existing directory: {value}") from error
    if not resolved.is_dir():
        raise ValidationError(f"{label} must be an existing directory: {value}")
    return resolved


def bridge_points_at_source(bridge: Path, source: Path | None) -> bool:
    """Return whether ``bridge`` resolves to the configured canonical source."""

    if source is None or not bridge.is_symlink():
        return False
    try:
        return bridge.resolve(strict=True) == Path(source).expanduser().resolve(strict=True)
    except (OSError, RuntimeError):
        return False


def require_resolved_mcp(
    config: RuntimeConfig, mcp_servers: Mapping[str, McpConfig], where: str
) -> None:
    """Require every selected MCP to have one caller-resolved definition."""

    if not isinstance(mcp_servers, Mapping):
        raise ValidationError(f"codex {where} requires a resolved mcp_servers mapping")
    for name in config.mcp:
        try:
            definition = mcp_servers[name]
        except (KeyError, TypeError) as error:
            raise ValidationError(f"codex mcp reference is not configured: {name}") from error
        if not isinstance(definition, McpConfig):
            raise ValidationError(f"codex mcp reference is not resolved: {name}")


def build_environment(binary: Path, home: Path) -> dict[str, str]:
    """Return the fully replaced environment for one Codex child process.

    ``binary`` is the configured Codex executable and must be absolute, so a
    child never depends on the collector's working directory; its parent
    directory is prefixed to ``PATH`` verbatim, without resolving symlinks, so
    a version-managed layout (nvm, Homebrew Cellar) keeps working without this
    module hard-coding a Node or package version.  ``home`` is the runtime home
    that owns the child's state -- either the base runtime home or one
    account-specific home -- and both ``HOME`` and ``CODEX_HOME`` point at it;
    no other variable is copied from the collector, except uv's existing
    managed-install root when present, so managed Python remains discoverable
    after ``HOME`` is replaced.

    ``PATH`` preserves the inherited entries in their original order and
    deduplicates them, so repeated launches cannot inflate the value, and it
    never contains an empty entry, which ``exec`` would read as the current
    directory. Nonempty ``os.defpath`` entries follow as fallback paths;
    a missing or blank inherited ``PATH`` contributes no entries.

    Raises:
        ValidationError: If ``binary`` is not an absolute path.
    """

    executable = Path(binary)
    if not executable.is_absolute():
        raise ValidationError(f"codex binary must be an absolute path: {binary}")
    home_text = str(home)
    return {
        "CODEX_HOME": home_text,
        "HOME": home_text,
        "PATH": _child_path(str(executable.parent)),
        **managed_uv_python_environment(),
    }


def developer_config_lines(config: RuntimeConfig) -> tuple[str, ...]:
    """Return native config lines needed by a selected developer environment."""

    return ("allow_login_shell = false", "") if config.environment is not None else ()


def developer_approval_fields(config: RuntimeConfig, write: bool) -> dict[str, str | None]:
    """Return the retained-review policy for a write-capable developer run."""

    if write and config.environment is not None:
        return {"approval_policy": "on-request", "approvals_reviewer": "auto_review"}
    return {"approval_policy": "never", "approvals_reviewer": None}


def prepared_environment(
    binary: Path,
    home: Path,
    config: RuntimeConfig,
    workdir: Path,
    *,
    refresh: bool = True,
) -> dict[str, str]:
    """Return the Codex child environment with its managed command policy.

    The selected developer environment augments the isolated Codex baseline.
    Private refusal shims lead ``PATH`` for normal shell lookup, while native
    ``.rules`` also deny each bare command and resolved executable path.
    """

    environment = developer_environment(build_environment(binary, home), config, workdir)
    denied_commands = config.environment.denied_commands if config.environment is not None else ()
    policy_directory = home / "command-refusals"
    if refresh:
        command_policy = materialize_refusal_commands(
            denied_commands,
            policy_directory,
            environment=environment,
        )
        write_managed_file(
            home,
            "rules/agent-run-command-policy.rules",
            render_codex_denial_rules(
                denied_commands,
                command_paths=tuple(command_policy.resolved_commands.values()),
            ),
        )
    elif not policy_directory.is_dir() or any(
        not (policy_directory / name).is_file()
        for name in (".agent-run-command-policy.json", *denied_commands)
    ):
        raise ValidationError("codex resume command policy is missing")
    environment["PATH"] = os.pathsep.join((str(policy_directory), environment["PATH"]))
    return environment


def thread_grant_params(
    cwd: str, model: str, sandbox_mode: str, approval_policy: str,
    roots: tuple[str, ...], network_access: bool, approvals_reviewer: str | None = None,
) -> dict[str, object]:
    """Return the shared ``thread/start``/``thread/resume`` grant fields.

    The installed 0.153.4 experimental schema's ``sandbox`` field on both
    ``ThreadStartParams`` and ``ThreadResumeParams`` is the plain kebab-case
    enum string; neither accepts an ``effort``, ``mcpServers``, or ``skills``
    field (effort belongs on ``TurnStartParams``; MCPs/skills come from the
    generated native config the app-server already reads). A requested
    network grant is instead conveyed through ``config``'s dotted
    ``sandbox_workspace_write.network_access``, verified live to yield
    ``networkAccess: true`` on the echoed thread. A selected reviewer is sent
    only for the developer write contract that requires automatic review.
    """

    params: dict[str, object] = {
        "cwd": cwd, "model": model, "sandbox": sandbox_mode,
        "approvalPolicy": approval_policy, "runtimeWorkspaceRoots": list(roots),
    }
    if network_access and sandbox_mode == "workspace-write":
        params["config"] = {"sandbox_workspace_write": {"network_access": True}}
    if approvals_reviewer is not None:
        params["approvalsReviewer"] = approvals_reviewer
    return params


def _child_path(launcher_directory: str) -> str:
    """Return the child ``PATH`` with ``launcher_directory`` leading.

    Inherited ``PATH`` entries follow in first-seen order, then the nonempty
    ``os.defpath`` entries as fallback paths.
    Duplicate and empty entries are dropped; the launcher directory is kept
    even when the inherited path already contains it, so the executable's own
    package always wins resolution order.
    """

    entries: list[str] = []
    for candidate in (launcher_directory, os.environ.get("PATH"), os.defpath):
        for entry in (candidate or "").split(os.pathsep):
            if entry and entry not in entries:
                entries.append(entry)
    return os.pathsep.join(entries)
