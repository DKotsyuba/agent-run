"""Owner-configured command denials for managed agent environments.

This module provides ordinary-use PATH refusal shims and the small native rule
representations supported by individual agent CLIs.  It deliberately does not
claim to confine absolute-path execution, aliases, or arbitrary machine code.
"""

from __future__ import annotations

import json
import os
import re
import stat
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping, Sequence

from ..errors import ValidationError

_COMMAND_NAME = re.compile(r"[A-Za-z0-9_][A-Za-z0-9._+-]*\Z")
_MARKER_NAME = ".agent-run-command-policy.json"
_MARKER_VERSION = 1
_REFUSAL = "#!/bin/sh\nprintf '%s\\n' 'agent-run: command denied by owner policy' >&2\nexit 126\n"


@dataclass(frozen=True)
class MaterializedCommandPolicy:
    """A private refusal directory and the host commands it resolved.

    ``directory`` is prepended to an agent's ordinary ``PATH`` by the caller.
    ``resolved_commands`` records the first executable path for each denied
    name. Native renderers include that path and its resolved symlink target;
    these are command rules, not an OS confinement guarantee.
    """

    directory: Path
    resolved_commands: Mapping[str, Path]


def validate_denied_commands(commands: Sequence[str]) -> tuple[str, ...]:
    """Return sorted unique bare command names or reject unsafe policy input.

    Names must be non-empty ASCII command basenames without separators,
    whitespace, shell syntax, or traversal.  The deterministic result avoids
    duplicate shims and makes rendered native policies reproducible.
    """

    validated: set[str] = set()
    for command in commands:
        if not isinstance(command, str) or _COMMAND_NAME.fullmatch(command) is None:
            raise ValidationError(f"denied command must be a bare command name: {command!r}")
        validated.add(command)
    return tuple(sorted(validated))


def materialize_refusal_commands(
    commands: Sequence[str],
    directory: Path,
    *,
    search_paths: Sequence[Path | str] | None = None,
    environment: Mapping[str, str] | None = None,
) -> MaterializedCommandPolicy:
    """Safely refresh private PATH refusal entries for denied command names.

    ``directory`` must be absent or an earlier directory created by this
    provider; symlinks, user-owned directories, and altered managed entries
    are rejected.  ``search_paths`` takes precedence over ``environment``'s
    ``PATH`` and is used only to report resolved executable targets.  Each shim
    exits 126 without invoking its target.  It intercepts only normal PATH
    lookup, not aliases, direct absolute paths, or arbitrary executable code.
    """

    denied = validate_denied_commands(commands)
    root = Path(directory)
    _prepare_directory(root)
    old_denied = _read_marker(root)
    for command in old_denied:
        if command not in denied:
            _remove_managed_refusal(root / command)
    for command in denied:
        _write_managed_refusal(root / command)
    _write_marker(root, denied)
    return MaterializedCommandPolicy(root, _resolve_commands(denied, search_paths, environment))


def render_codex_denial_rules(
    commands: Sequence[str], *, command_paths: Sequence[Path | str] = ()
) -> str:
    """Render deterministic Codex ``.rules`` forbidden-prefix rules.

    Both bare names and explicitly supplied absolute paths are included.  The
    rules express Codex's native outside-sandbox approval policy; callers must
    not represent them as an OS-level execution guarantee.
    """

    patterns = _native_command_patterns(commands, command_paths)
    return "\n".join(
        "prefix_rule(pattern=[%s], decision=\"forbidden\", justification=\"Denied by owner command policy\")"
        % json.dumps(pattern)
        for pattern in patterns
    ) + ("\n" if patterns else "")


def render_claude_denials(
    commands: Sequence[str], *, command_paths: Sequence[Path | str] = ()
) -> tuple[str, ...]:
    """Render Claude ``--disallowedTools`` Bash entries for exact commands.

    Every command has an argument-free and an argument-bearing pattern, so a
    similarly named command such as ``gh-extra`` is not denied accidentally.
    """

    return _bash_denials(_native_command_patterns(commands, command_paths))


def render_qwen_denials(
    commands: Sequence[str], *, command_paths: Sequence[Path | str] = ()
) -> tuple[str, ...]:
    """Render Qwen ``permissions.deny`` Bash entries for exact commands.

    Qwen's documented precedence makes these native deny entries stronger than
    ask or allow entries; callers insert the returned strings in that array.
    """

    return _bash_denials(_native_command_patterns(commands, command_paths))


def _prepare_directory(directory: Path) -> None:
    """Create a private managed directory or verify its ownership marker."""

    try:
        info = directory.lstat()
    except FileNotFoundError:
        directory.mkdir(mode=0o700, parents=True)
        return
    if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
        raise ValidationError("command-policy directory must be a real directory")
    if not (directory / _MARKER_NAME).is_file():
        raise ValidationError("command-policy directory is not managed by agent-run")


def _read_marker(directory: Path) -> tuple[str, ...]:
    """Read and validate this provider's policy marker from ``directory``."""

    marker = directory / _MARKER_NAME
    try:
        if not stat.S_ISREG(marker.lstat().st_mode):
            raise ValidationError("command-policy marker must be a regular file")
        raw = json.loads(marker.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return ()
    except (json.JSONDecodeError, UnicodeError) as error:
        raise ValidationError("command-policy marker is invalid") from error
    if not isinstance(raw, dict) or raw.get("version") != _MARKER_VERSION or not isinstance(raw.get("commands"), list):
        raise ValidationError("command-policy marker is invalid")
    return validate_denied_commands(raw["commands"])


def _write_marker(directory: Path, commands: tuple[str, ...]) -> None:
    """Atomically replace the provider marker after a successful refresh."""

    _atomic_write(directory / _MARKER_NAME, json.dumps({"version": _MARKER_VERSION, "commands": commands}) + "\n")


def _write_managed_refusal(path: Path) -> None:
    """Atomically create or replace one verified regular refusal shim."""

    if path.exists() or path.is_symlink():
        info = path.lstat()
        if stat.S_ISLNK(info.st_mode) or not stat.S_ISREG(info.st_mode) or path.read_text(encoding="utf-8") != _REFUSAL:
            raise ValidationError(f"refusing to replace unmanaged command-policy entry: {path.name}")
    _atomic_write(path, _REFUSAL, mode=0o700)


def _remove_managed_refusal(path: Path) -> None:
    """Remove only an unchanged regular refusal shim formerly in the marker."""

    try:
        info = path.lstat()
    except FileNotFoundError:
        return
    if stat.S_ISLNK(info.st_mode) or not stat.S_ISREG(info.st_mode) or path.read_text(encoding="utf-8") != _REFUSAL:
        raise ValidationError(f"refusing to remove unmanaged command-policy entry: {path.name}")
    path.unlink()


def _atomic_write(path: Path, content: str, *, mode: int = 0o600) -> None:
    """Write ``content`` through a private sibling then replace a regular file."""

    temporary = path.with_name(f".{path.name}.new")
    if temporary.exists() or temporary.is_symlink():
        raise ValidationError(f"command-policy temporary already exists: {temporary.name}")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, mode)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            stream.write(content)
        os.replace(temporary, path)
    except BaseException:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass
        raise


def _resolve_commands(
    commands: tuple[str, ...], search_paths: Sequence[Path | str] | None, environment: Mapping[str, str] | None
) -> dict[str, Path]:
    """Resolve each first executable from explicit paths or a supplied PATH."""

    if search_paths is None:
        source = os.environ if environment is None else environment
        search_paths = tuple(part for part in source.get("PATH", "").split(os.pathsep) if part)
    resolved: dict[str, Path] = {}
    for command in commands:
        for directory in search_paths:
            if not Path(directory).is_absolute():
                continue
            candidate = Path(directory) / command
            if candidate.is_file() and os.access(candidate, os.X_OK):
                resolved[command] = candidate
                break
    return resolved


def _native_command_patterns(commands: Sequence[str], command_paths: Sequence[Path | str]) -> tuple[str, ...]:
    """Return bare names, absolute paths and symlink targets for native rules."""

    patterns = set(validate_denied_commands(commands))
    for command_path in command_paths:
        path = Path(command_path)
        if not path.is_absolute():
            raise ValidationError(f"native command path must be absolute: {command_path!r}")
        patterns.add(str(path))
        try:
            patterns.add(str(path.resolve()))
        except (OSError, RuntimeError) as error:
            raise ValidationError(f"cannot resolve native command path: {command_path!r}") from error
    return tuple(sorted(patterns))


def _bash_denials(patterns: tuple[str, ...]) -> tuple[str, ...]:
    """Expand exact command patterns into no-argument and argument forms."""

    return tuple(entry for pattern in patterns for entry in (f"Bash({pattern})", f"Bash({pattern} *)"))
