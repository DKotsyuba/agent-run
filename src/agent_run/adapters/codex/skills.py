"""Managed Codex skill-directory lifecycle helpers."""

from __future__ import annotations

from pathlib import Path

from ...errors import PathEscapeError


def prune_skills(home: Path, selected: frozenset[str]) -> None:
    """Remove deselected adapter-owned skill directories below ``home``.

    Only direct ``skills/`` children containing a regular managed ``SKILL.md``
    are removed. Symlinks and directories carrying runtime-owned files remain
    untouched; an escaped skills-root symlink raises ``PathEscapeError``.
    """
    skills_root = home / "skills"
    if skills_root.is_symlink():
        raise PathEscapeError(f"codex skills root must not be a symlink: {skills_root}")
    if not skills_root.is_dir():
        return
    for child in sorted(skills_root.iterdir()):
        if child.name in selected or child.is_symlink() or not child.is_dir():
            continue
        managed = child / "SKILL.md"
        if managed.is_symlink() or not managed.is_file():
            continue
        managed.unlink()
        try:
            child.rmdir()
        except OSError:
            pass
