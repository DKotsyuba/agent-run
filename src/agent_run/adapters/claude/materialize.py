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
from ..home import content_hash, create_symlink_bridge, write_managed_file

__all__ = ["render_mcp_config", "render_plugin_dirs", "render_settings"]


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
    home: Path, names: tuple[str, ...], mcp_servers: Mapping[str, McpConfig]
) -> str:
    """Write the strict MCP config for the selected names only.

    Fails closed when a configured name has no resolved definition rather
    than emitting a non-functional entry.
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
    return write_managed_file(home, "mcp/mcp-config.json", json.dumps({"mcpServers": servers}, sort_keys=True))


def render_plugin_dirs(home: Path, skills_root: Path, names: tuple[str, ...]) -> str:
    """Generate one plugin directory per selected skill.

    Every top-level child of the owner-authored skill directory (SKILL.md,
    plus any scripts/, references/, assets/, or other sibling) is bridged
    into the generated plugin's skill directory through the same validated
    symlink bridge, not just the manifest file.
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
        children = []
        for child in sorted(skill_dir.iterdir()):
            create_symlink_bridge(home, f"plugins/{name}/skills/{name}/{child.name}", child)
            children.append(child.name)
        digests.append(f"{name}:{manifest_digest}:{','.join(children)}")
    return content_hash(",".join(digests)) if digests else content_hash("no_skills")
