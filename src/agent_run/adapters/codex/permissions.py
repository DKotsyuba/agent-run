"""Native Codex MCP approval and workspace-root translation."""

from __future__ import annotations

import re
import sys
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path

from ...config import McpConfig, RuntimeConfig, RuntimeHookConfig
from ...errors import ValidationError
from ...profiles import normalize_read_roots
from .toml import toml_array, toml_string


#: Shared user-visible name for the broad working-projects permission profile.
PROJECTS_PROFILE = "Projects"


class VerificationError(ValidationError):
    """Effective app-server parameters do not match the requested launch plan."""


@dataclass(frozen=True)
class EffectiveTurnParams:
    """Security-relevant parameters requested for one Codex thread.

    ``model`` and ``cwd`` identify execution, ``roots`` bind the thread's runtime
    workspace roots, ``writable_roots`` bind its effective write scope, and
    ``sandbox`` records the expected effective type.
    ``approval_policy`` plus optional ``permission_profile`` bind review and
    named-profile provenance. ``network_access`` requires an explicit true echo.
    Instances are immutable and contain no credentials.
    """

    model: str
    cwd: str
    roots: tuple[str, ...]
    sandbox: str
    approval_policy: str
    writable_roots: tuple[str, ...]
    network_access: bool = False
    permission_profile: str | None = None


#: Camel-case app-server sandbox echo types mapped to legacy adapter names.
_SANDBOX_ECHO_TYPES: Mapping[str, str] = {
    "readOnly": "read-only",
    "workspaceWrite": "workspace-write",
    "dangerFullAccess": "danger-full-access",
}


def _normalized_sandbox_echo(value: object) -> object:
    """Reduce a legacy string or beta object sandbox echo to kebab case."""

    if isinstance(value, Mapping):
        return _SANDBOX_ECHO_TYPES.get(value.get("type"), value)
    return value


def _normalized_roots_echo(actual: Mapping[str, object]) -> tuple[str, ...]:
    """Return legacy or beta runtime workspace roots from one thread echo."""

    if "roots" in actual:
        return tuple(actual.get("roots") or ())
    return tuple(actual.get("runtimeWorkspaceRoots") or ())


def _normalized_writable_roots_echo(actual: Mapping[str, object]) -> tuple[str, ...]:
    """Return explicit or sandbox-implied writable roots from a thread echo."""

    if "writableRoots" in actual:
        return tuple(actual.get("writableRoots") or ())
    sandbox = actual.get("sandbox")
    if not isinstance(sandbox, Mapping):
        return ()
    nested = tuple(sandbox.get("writableRoots") or ())
    if nested:
        return nested
    if sandbox.get("type") == "workspaceWrite":
        cwd = actual.get("cwd")
        if isinstance(cwd, str) and cwd:
            return (cwd,)
    return nested


def verify_effective_params(
    expected: EffectiveTurnParams, actual: Mapping[str, object]
) -> None:
    """Refuse thread echoes that drift from requested security parameters.

    ``expected`` is the adapter's immutable launch contract and ``actual`` is
    the app-server thread response. Named profiles must echo the exact
    ``activePermissionProfile.id``. Sandbox type, approval policy, readable
    runtime roots, writable roots, and requested network access remain
    independently verified. Any mismatch raises ``VerificationError`` through
    the caller's shared ``ValidationError`` boundary.
    """

    scalar_checks = (
        ("model", expected.model),
        ("cwd", expected.cwd),
        ("sandbox", expected.sandbox),
        ("approvalPolicy", expected.approval_policy),
    )
    for key, wanted in scalar_checks:
        got = actual.get(key)
        compare = _normalized_sandbox_echo(got) if key == "sandbox" else got
        if compare != wanted:
            raise VerificationError(
                f"codex thread/start {key} mismatch: expected {wanted!r}, got {got!r}"
            )
    if expected.permission_profile is not None:
        active = actual.get("activePermissionProfile")
        if not isinstance(active, Mapping) or active.get("id") != expected.permission_profile:
            raise VerificationError(
                "codex thread/start permission profile mismatch: expected "
                f"{expected.permission_profile!r}, got {active!r}"
            )
    got_roots = _normalized_roots_echo(actual)
    if got_roots != expected.roots:
        raise VerificationError(
            f"codex thread/start roots mismatch: expected {expected.roots!r}, got {got_roots!r}"
        )
    got_writable = _normalized_writable_roots_echo(actual)
    if got_writable != expected.writable_roots:
        raise VerificationError(
            "codex thread/start writableRoots mismatch: expected "
            f"{expected.writable_roots!r}, got {got_writable!r}"
        )
    if expected.network_access:
        sandbox = actual.get("sandbox")
        if not isinstance(sandbox, Mapping) or sandbox.get("networkAccess") is not True:
            raise VerificationError("codex thread/start did not enable requested network access")


def render_permission_profile(config: RuntimeConfig) -> list[str]:
    """Return a generated ``Projects`` profile for a configured workspace root.

    ``config.workspace_root`` is the operator-authorized working tree. Missing
    configuration returns an empty list. The profile inherits Codex platform
    defaults, writes the working tree, denies common secret files, and uses the
    configured shell-network setting without a root-wide read deny.
    """

    if config.workspace_root is None:
        return []
    root = toml_string(str(config.workspace_root))
    return [
        f'default_permissions = "{PROJECTS_PROFILE}"',
        "",
        f"[permissions.{PROJECTS_PROFILE}]",
        'extends = ":workspace"',
        "",
        f"[permissions.{PROJECTS_PROFILE}.workspace_roots]",
        f"{root} = true",
        "",
        f'[permissions.{PROJECTS_PROFILE}.filesystem.":workspace_roots"]',
        '"." = "write"',
        '"**/.env" = "deny"',
        '"**/.env.*" = "deny"',
        '"**/*.pem" = "deny"',
        '"**/*.key" = "deny"',
        "",
        f"[permissions.{PROJECTS_PROFILE}.network]",
        f"enabled = {str(config.workspace_network).lower()}",
        "",
    ]


def thread_grant_params(
    cwd: str,
    model: str,
    sandbox_mode: str,
    approval_policy: str,
    roots: tuple[str, ...],
    network_access: bool,
    approvals_reviewer: str | None = None,
    permission_profile: str | None = None,
) -> dict[str, object]:
    """Return thread start/resume fields for a profile or legacy sandbox.

    Named ``permission_profile`` sessions omit legacy ``sandbox`` and
    ``runtimeWorkspaceRoots`` so the generated ``default_permissions`` profile
    remains authoritative. Legacy read-only or network sessions retain the
    existing explicit sandbox and root contract. Reviewer routing is included
    only when configured.
    """

    params: dict[str, object] = {
        "cwd": cwd,
        "model": model,
        "approvalPolicy": approval_policy,
    }
    if permission_profile is None:
        params.update(
            {"sandbox": sandbox_mode, "runtimeWorkspaceRoots": list(roots)}
        )
        if network_access and sandbox_mode == "workspace-write":
            params["config"] = {
                "sandbox_workspace_write": {"network_access": True}
            }
    if approvals_reviewer is not None:
        params["approvalsReviewer"] = approvals_reviewer
    return params


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
