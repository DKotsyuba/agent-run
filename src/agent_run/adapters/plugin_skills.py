"""One source per skill name, shared by every runtime adapter.

agent-run ships owner-authored skill copies below ``<home>/skills/<runtime>/``
and can also declare plugins that carry skills of their own. When both offer
the same name the child sees the skill twice -- claude loads two plugin dirs
exporting one name, opencode logs ``duplicate skill name`` and drops one of
them, and neither host says which copy won. So a declared plugin that ships
``skills/<name>/SKILL.md`` owns that name outright, and the copy below the
runtime skills root is simply not read.

Ownership is resolved from the declared plugin directories only. Nothing here
scans a global or ambient skill location.
"""

from __future__ import annotations

from pathlib import Path
from typing import Iterable, Mapping

from ..errors import ValidationError

__all__ = ["local_skill_names", "plugin_skill_dir", "skill_dirs"]


def plugin_skill_dir(plugins: Iterable[Path], name: str) -> Path | None:
    """Return the declared plugin's directory shipping ``name``, if one does.

    Two plugins claiming one skill name is a configuration defect rather than a
    precedence question, so it fails closed instead of picking a winner.
    """

    found: Path | None = None
    for plugin in plugins:
        candidate = Path(plugin) / "skills" / name
        if not (candidate / "SKILL.md").is_file():
            continue
        if found is not None:
            raise ValidationError(
                f"skill {name!r} is shipped by two declared plugins: {found} and {candidate}"
            )
        found = candidate
    return found


def skill_dirs(
    plugins: Iterable[Path], skills_root: Path, names: Iterable[str]
) -> Mapping[str, Path]:
    """Map every selected skill name to the one directory it is read from."""

    plugins = tuple(plugins)
    return {
        name: plugin_skill_dir(plugins, name) or Path(skills_root) / name
        for name in names
    }


def local_skill_names(plugins: Iterable[Path], names: Iterable[str]) -> tuple[str, ...]:
    """Names the runtime must ship itself: those no declared plugin exports.

    A host that loads declared plugins directly (claude) would otherwise export
    the same skill twice -- once from the plugin, once from the directory this
    adapter generates for it.
    """

    plugins = tuple(plugins)
    return tuple(name for name in names if plugin_skill_dir(plugins, name) is None)
