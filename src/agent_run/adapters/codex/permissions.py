"""Native Codex MCP approval and workspace-root translation."""

from __future__ import annotations

import re
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path

from ...config import McpConfig, RuntimeConfig, RuntimeHookConfig
from ...errors import ValidationError
from ...profiles import normalize_read_roots
from .toml import toml_array, toml_string


def render_mcp_config(
    config: RuntimeConfig, mcp_servers: Mapping[str, McpConfig]
) -> list[str]:
    """Return native TOML lines for the runtime's selected MCP servers.

    ``config`` supplies ordered server identifiers and ``mcp_servers`` contains
    their validated definitions. The returned mutable line list contains no
    credential values; environment entries remain names. Non-default approval
    modes are rendered explicitly. Missing definitions raise ``KeyError`` only
    if the caller skipped its normal resolved-MCP validation.
    """

    lines: list[str] = []
    for name in sorted(config.mcp):
        server = mcp_servers[name]
        lines.extend(
            (
                f"[mcp_servers.{name}]",
                f"command = {toml_string(str(server.command))}",
                f"args = {toml_array(server.args)}",
            )
        )
        if server.env_from:
            lines.append(f"env_vars = {toml_array(server.env_from)}")
        if server.approval_mode != "auto":
            lines.append(
                f"default_tools_approval_mode = {toml_string(server.approval_mode)}"
            )
        lines.append("")
    return lines


def permission_request_hook(
    config: RuntimeConfig, mcp_servers: Mapping[str, McpConfig]
) -> RuntimeHookConfig | None:
    """Build the native allow hook for MCP servers configured as trusted.

    ``config`` selects MCP identifiers and ``mcp_servers`` maps them to validated
    definitions. No trusted servers returns ``None``. The command uses the
    installed agent-run interpreter in isolated mode, forwards only identifiers,
    and matches hyphenated plus Codex-normalized underscore namespaces. It never
    grants Bash or filesystem operations.
    """

    trusted = tuple(
        name
        for name in sorted(config.mcp)
        if mcp_servers[name].approval_mode == "approve"
    )
    if not trusted:
        return None
    variants = sorted(
        {variant for name in trusted for variant in (name, name.replace("-", "_"))}
    )
    matcher = "^(?:" + "|".join(re.escape(f"mcp__{name}__") for name in variants) + ")"
    command = (
        sys.executable,
        "-I",
        "-m",
        "agent_run.adapters.codex.permission_request",
        *(argument for name in trusted for argument in ("--allow-mcp", name)),
    )
    return RuntimeHookConfig("PermissionRequest", command, matcher)


def workspace_roots(
    workdir: Path,
    read_roots: Sequence[Path],
    configured_root: Path | None,
    write: bool,
) -> tuple[tuple[str, ...], tuple[str, ...]]:
    """Return effective readable and writable Codex workspace roots.

    ``workdir`` and every ``read_roots`` entry must already be resolved absolute
    paths. ``configured_root`` is an optional operator-authorized project tree;
    it replaces the workdir only for write-capable sessions and must contain the
    workdir. Read-only sessions ignore it. A write session with any normalized
    external read root raises ``ValidationError`` because current Codex
    app-server cannot express it without widening write access.
    """

    if write and configured_root is not None and not workdir.is_relative_to(configured_root):
        raise ValidationError(
            "codex write workdir must be inside runtimes.codex.workspace_root"
        )
    root_seed = (
        (configured_root, *read_roots)
        if write and configured_root is not None
        else (workdir, *read_roots)
    )
    readable = tuple(str(root) for root in normalize_read_roots(root_seed))
    writable = (str(configured_root or workdir),) if write else ()
    if write and readable != writable:
        raise ValidationError(
            "codex workspace-write threads cannot grant external read roots; "
            "copy the material into the workdir and omit --read-root"
        )
    return readable, writable
