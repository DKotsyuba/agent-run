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
from pathlib import Path

from ...errors import ValidationError
from ..home import managed_uv_python_environment


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
