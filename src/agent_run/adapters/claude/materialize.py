"""Generated-asset rendering for the ``claude`` runtime home.

Every asset below the generated home is built only from ``RuntimeConfig``
and the owner-authored skill directories -- never from the caller's global
Claude settings, plugins, or MCP configuration.
"""

from __future__ import annotations

import json
import shlex
from pathlib import Path
from typing import Mapping

from ...config import McpConfig, RuntimeHookConfig
from ...errors import ValidationError
from ..home import content_hash, write_managed_file
from ..snapshots import snapshot_managed_tree, snapshot_selected_assets

__all__ = [
    "render_declared_plugin_snapshots",
    "render_mcp_config",
    "render_plugin_dirs",
    "render_settings",
]


def render_settings(home: Path, hooks: tuple[RuntimeHookConfig, ...]) -> str:
    """Write the generated settings.json holding only declared hooks."""

    grouped: dict[str, list[dict[str, object]]] = {}
    for hook in hooks:
        entry: dict[str, object] = {"hooks": [{"type": "command", "command": shlex.join(hook.command)}]}
        if hook.matcher is not None:
            entry["matcher"] = hook.matcher
        grouped.setdefault(hook.event, []).append(entry)
    settings = {"hooks": grouped} if grouped else {}
    return write_managed_file(home, "settings.json", json.dumps(settings, sort_keys=True))


def render_mcp_config(
    home: Path,
    names: tuple[str, ...],
    mcp_servers: Mapping[str, McpConfig],
    *,
    environment: Mapping[str, str] | None = None,
) -> str:
    """Write the strict MCP config for the selected names only.

    Fails closed when a configured name has no resolved definition rather
    than emitting a non-functional entry. ``environment`` is optional,
    nonsecret stdio child environment shared by selected servers; absent it
    preserves the ordinary descriptor shape.
    """

    if not names:
        return content_hash("no_mcp")
    servers: dict[str, object] = {}
    for name in names:
        server = mcp_servers.get(name)
        if server is None:
            raise ValidationError(f"no resolved MCP definition for runtimes.claude.mcp entry: {name}")
        servers[name] = {
            "type": server.transport,
            "command": str(server.command),
            "args": list(server.args),
        }
        if environment is not None:
            servers[name]["env"] = dict(environment)
    return write_managed_file(home, "mcp/mcp-config.json", json.dumps({"mcpServers": servers}, sort_keys=True))


def render_plugin_dirs(home: Path, skills_root: Path, names: tuple[str, ...]) -> str:
    """Generate one plugin directory per selected skill.

    Every directory and regular file in the owner-authored skill is copied into
    the generated plugin through the shared immutable snapshot contract. Source
    symlinks and special files fail closed; the returned revision covers paths,
    types, and bytes rather than names alone.
    """

    digests = []
    for name in sorted(names):
        skill_dir = skills_root / name
        manifest_source = skill_dir / "SKILL.md"
        if not manifest_source.exists():
            raise ValidationError(f"claude skill not found: {name}")
        manifest_digest = write_managed_file(
            home, f"plugins/{name}/.claude-plugin/plugin.json", json.dumps({"name": name}, sort_keys=True)
        )
        snapshot = snapshot_managed_tree(
            home, f"plugins/{name}/skills/{name}", skill_dir
        )
        digests.append(f"{name}:{manifest_digest}:{snapshot.sha256}")
    return content_hash(",".join(digests)) if digests else content_hash("no_skills")


def render_declared_plugin_snapshots(
    home: Path,
    plugins: tuple[Path, ...],
    declarations: Mapping[str, tuple[str, ...]],
) -> str:
    """Snapshot only explicitly declared plugin assets and return their revision.

    ``declarations`` is keyed by configured plugin basename. Missing declarations
    preserve the legacy live-plugin path and are fingerprinted as unsupported
    mutability. Declared assets use the shared no-follow selected-path snapshot;
    unknown names or duplicate configured basenames fail closed.
    """

    roots: dict[str, Path] = {}
    for plugin in plugins:
        if plugin.name in roots:
            raise ValidationError(f"claude plugin name is declared twice: {plugin.name}")
        roots[plugin.name] = plugin
    unknown = sorted(set(declarations) - set(roots))
    if unknown:
        raise ValidationError(
            "claude plugin snapshot names are not configured: " + ", ".join(unknown)
        )
    fingerprints: list[str] = []
    for name, plugin in roots.items():
        assets = declarations.get(name)
        if assets is None:
            fingerprints.append(f"{name}:live:{plugin}")
            continue
        snapshot = snapshot_selected_assets(
            home, f"declared-plugins/{name}", plugin, assets
        )
        fingerprints.append(f"{name}:snapshot:{snapshot.sha256}")
    return content_hash("\n".join(fingerprints)) if fingerprints else content_hash("no_plugins")
