"""Skill delivery for Qwen children.

Qwen Code has no Skill tool, so parity with the neighboring runtimes is a
file-delivery mechanism instead: every configured skill's ``SKILL.md`` is
copied beneath the generated home, and the injected context file is given
the absolute path so the model can open each one itself. Plugin-owned skill
names are resolved the same way :mod:`agent_run.adapters.codex.adapter` and
:mod:`agent_run.adapters.opencode.adapter` resolve them, through the shared
:mod:`agent_run.adapters.plugin_skills` module.
"""

from __future__ import annotations

from pathlib import Path
from typing import Iterable

from agent_run.adapters.home import write_managed_file
from agent_run.adapters.plugin_skills import skill_dirs
from agent_run.errors import PathEscapeError, ValidationError

__all__ = ["SKILLS_DIR_NAME", "materialize_skills", "skills_context_note"]

#: Directory below the generated home that holds delivered skill copies.
SKILLS_DIR_NAME = "skills"


def materialize_skills(
    home: Path, plugins: Iterable[Path], skills_root: Path, names: Iterable[str]
) -> dict[str, str]:
    """Copy every selected skill's ``SKILL.md`` beneath ``home`` and hash it.

    :param home: The adapter's generated, already-created home directory.
    :param plugins: Declared plugin directories that may own a skill name;
        a plugin-owned name is read from the plugin instead of
        ``skills_root`` (see :func:`agent_run.adapters.plugin_skills.skill_dirs`).
    :param skills_root: Absolute directory this runtime's unowned skill
        copies live under, one subdirectory per skill name.
    :param names: The configured skill names to deliver, in ``config.skills``
        order; duplicates are harmless since each name maps to one file.
    :returns: A mapping of skill name to the SHA-256 hex digest of the copied
        ``SKILL.md`` content, suitable for folding into a materialize
        fingerprint.
    :raises ValidationError: A selected skill's ``SKILL.md`` cannot be read
        from its resolved source directory.
    """

    names = tuple(names)
    sources = skill_dirs(plugins, skills_root, names)
    hashes: dict[str, str] = {}
    for name in names:
        source = sources[name] / "SKILL.md"
        try:
            text = source.read_text(encoding="utf-8")
        except OSError as error:
            raise ValidationError(f"qwen skill is not available: {name}: {error}") from error
        hashes[name] = write_managed_file(home, f"{SKILLS_DIR_NAME}/{name}/SKILL.md", text)
    _prune_skills(Path(home), frozenset(names))
    return hashes


def _prune_skills(home: Path, selected: frozenset[str]) -> None:
    """Drop adapter-owned skill directories that are no longer selected.

    Mirrors :func:`agent_run.adapters.codex.adapter._prune_skills`: only
    direct children below ``skills/`` that carry a managed ``SKILL.md`` are
    touched, so runtime-owned state below the generated home survives.
    """

    skills_root = home / SKILLS_DIR_NAME
    if skills_root.is_symlink():
        raise PathEscapeError(f"qwen skills root must not be a symlink: {skills_root}")
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
            # The runtime kept unrelated files below this skill; leave them.
            pass


def skills_context_note(home: Path, names: Iterable[str]) -> str:
    """Return the context-file paragraph pointing at delivered skill files.

    :param home: The adapter's generated home directory; the note names the
        absolute ``<home>/skills`` path so the child can read it verbatim.
    :param names: The configured skill names delivered by
        :func:`materialize_skills`.
    :returns: Empty string when ``names`` is empty (no paragraph is added),
        otherwise a trailing-newline-terminated paragraph naming the
        absolute skills directory and the sorted configured skill names.
    """

    names = tuple(names)
    if not names:
        return ""
    skills_path = Path(home) / SKILLS_DIR_NAME
    listed = ", ".join(sorted(names))
    return (
        "\nThis runtime has no Skill tool. Configured skills are delivered as "
        f"files under {skills_path}. Before following a skill's contract, read "
        f"its {skills_path}/<name>/SKILL.md. Configured skills: {listed}.\n"
    )
