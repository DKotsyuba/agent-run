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
from ..environment import host_environment
from ..home import write_managed_file


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


def auth_bridge(config: RuntimeConfig) -> tuple[Path, str] | None:
    """Return the selected Codex auth source and generated-home target.

    Explicit file-link declarations are validated. Without one, the host
    ``CODEX_HOME`` or ``~/.codex/auth.json`` is used when it exists; a missing
    native account returns ``None`` without reading credential bytes.
    """

    if config.auth is None:
        configured = os.environ.get("CODEX_HOME")
        root = Path(configured).expanduser() if configured else Path.home() / ".codex"
        source = root / "auth.json"
        return (source, "auth.json") if source.is_file() else None
    if config.auth.source is None:
        raise ValidationError("codex file_link auth source is missing")
    if config.auth.target is None:
        raise ValidationError("codex file_link auth target is missing")
    return config.auth.source, config.auth.target


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


def build_environment(
    binary: Path, home: Path, *, allowed_secret_names: tuple[str, ...] = ()
) -> dict[str, str]:
    """Return the inherited host environment for one Codex child process.

    ``binary`` must be absolute. ``home`` replaces only ``HOME`` and
    ``CODEX_HOME`` so generated Codex configuration stays separate. PATH,
    locales, SDKs and toolchains come from the service host. Credential-shaped
    variables are inherited only when ``allowed_secret_names`` selected them.

    Raises:
        ValidationError: If ``binary`` is not an absolute path.
    """

    executable = Path(binary)
    if not executable.is_absolute():
        raise ValidationError(f"codex binary must be an absolute path: {binary}")
    home_text = str(home)
    return host_environment(
        {"CODEX_HOME": home_text, "HOME": home_text},
        allowed_secret_names=allowed_secret_names,
    )


def approval_fields(write: bool) -> dict[str, str | None]:
    """Return approval settings for the effective write grant."""

    if write:
        return {"approval_policy": "on-request", "approvals_reviewer": "auto_review"}
    return {"approval_policy": "never", "approvals_reviewer": None}


def prepared_environment(
    binary: Path,
    home: Path,
    *,
    mcp_environment_names: tuple[str, ...] = (),
    denied_commands: tuple[str, ...] = (),
    refresh: bool = True,
) -> dict[str, str]:
    """Return the Codex child environment with its managed command policy.

    ``mcp_environment_names`` may cross the credential filter. Optional legacy
    command denials remain active during config migration.
    """

    environment = build_environment(
        binary, home, allowed_secret_names=mcp_environment_names
    )
    if not denied_commands:
        return environment
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
