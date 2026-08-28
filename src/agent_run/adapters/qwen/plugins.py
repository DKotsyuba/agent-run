"""Deliver declared plugin files into the generated Qwen home.

agent-run's plugin declarations exist so a hook command can reference a
script inside a plugin directory without hard-coding a checkout path on the
command line (see :func:`agent_run.adapters.codex.plugins.expand` for the
same idea in the codex adapter). Qwen has no marketplace or hook-trust-digest
concept of its own to reproduce: a configured hook command just needs the
referenced file to resolve, so this module copies each declared plugin
verbatim into the generated home and resolves ``{plugin:NAME}`` tokens
against that copy, keyed by the plugin directory's own basename.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Iterable, Mapping

from agent_run.adapters.home import content_hash, write_managed_file
from agent_run.errors import ValidationError

__all__ = ["PLUGINS_DIR_NAME", "expand", "install"]

#: Directory below the generated home that holds delivered plugin copies.
PLUGINS_DIR_NAME = "plugins"

_PLUGIN_TOKEN = re.compile(r"\{plugin:([A-Za-z0-9][A-Za-z0-9_.+-]*)\}")
_SKIP_DIRS = frozenset({".git", "__pycache__"})


def _copy_tree(home: Path, relative: str, directory: Path) -> str:
    """Copy one plugin directory's real files beneath ``home`` and hash them.

    Mirrors :func:`agent_run.adapters.codex.plugins._copy_tree`: symlinks and
    non-regular files are skipped, so the child never runs a file the
    declared plugin directory did not itself contain as a real file.
    """

    digests: list[str] = []
    for source in sorted(directory.rglob("*")):
        parts = source.relative_to(directory).parts
        if any(part in _SKIP_DIRS for part in parts) or source.is_symlink() or not source.is_file():
            continue
        try:
            payload = source.read_bytes()
        except OSError as error:
            raise ValidationError(f"cannot read qwen plugin file {source}: {error}") from error
        digest = write_managed_file(home, f"{relative}/{'/'.join(parts)}", payload)
        digests.append(f"{'/'.join(parts)}:{digest}")
    if not digests:
        raise ValidationError(f"qwen plugin directory has no readable files: {directory}")
    return content_hash("\n".join(digests))


def install(home: Path, plugins: Iterable[Path]) -> tuple[dict[str, Path], str]:
    """Copy every declared plugin directory into the generated home.

    :param home: The adapter's generated, already-created home directory.
    :param plugins: Declared plugin directories, in ``config.plugins`` order.
    :returns: ``(roots, fingerprint)``. ``roots`` maps each plugin
        directory's basename to its copy's absolute path below ``home``, for
        :func:`expand` to resolve ``{plugin:NAME}`` hook-command tokens
        against. ``fingerprint`` folds every copied file's digest, suitable
        for a materialize revision hash.
    :raises ValidationError: Two declared plugin directories share one
        basename, or a declared directory has no readable files.
    """

    roots: dict[str, Path] = {}
    fingerprint: list[str] = []
    for directory in plugins:
        name = Path(directory).name
        if name in roots:
            raise ValidationError(f"qwen plugin name is declared twice: {name}")
        relative = f"{PLUGINS_DIR_NAME}/{name}"
        tree_digest = _copy_tree(Path(home), relative, Path(directory))
        roots[name] = Path(home) / relative
        fingerprint.append(f"{name}:{tree_digest}")
    digest = content_hash("\n".join(fingerprint)) if fingerprint else content_hash("no_plugins")
    return roots, digest


def expand(command: Iterable[str], roots: Mapping[str, Path]) -> tuple[str, ...]:
    """Resolve ``{plugin:NAME}`` tokens to that plugin's copy inside the home.

    :raises ValidationError: A token names a plugin that was not declared
        (and therefore has no entry in ``roots``), so the command would
        otherwise silently reference a path nothing installed.
    """

    def replace(match: re.Match[str]) -> str:
        try:
            return str(roots[match.group(1)])
        except KeyError:
            raise ValidationError(
                f"qwen hook command references {match.group(0)}, which is not a declared plugin"
            ) from None

    return tuple(_PLUGIN_TOKEN.sub(replace, word) for word in command)
