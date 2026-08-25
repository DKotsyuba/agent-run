"""Named role profiles and their effective permissions."""

from __future__ import annotations

import re
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from .config import ProfilesConfig
from .errors import PathEscapeError, ValidationError


_PROFILE_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]*\Z")


@dataclass(frozen=True)
class AgentProfile:
    name: str
    body: str
    write: bool
    read_roots: tuple[Path, ...] = ()


def normalize_read_roots(values: Iterable[str | Path]) -> tuple[Path, ...]:
    """Resolve existing directories to a deduplicated minimal antichain."""

    resolved: set[Path] = set()
    for index, value in enumerate(values):
        try:
            path = Path(value).expanduser()
        except (TypeError, RuntimeError) as error:
            raise ValidationError(
                f"read_roots[{index}] must be an absolute existing directory"
            ) from error
        if not path.is_absolute():
            raise ValidationError(f"read_roots[{index}] must be an absolute existing directory")
        try:
            path = path.resolve(strict=True)
        except (OSError, RuntimeError) as error:
            raise ValidationError(
                f"read_roots[{index}] must be an absolute existing directory"
            ) from error
        if not path.is_dir():
            raise ValidationError(f"read_roots[{index}] must be an absolute existing directory")
        resolved.add(path)

    roots: list[Path] = []
    for path in sorted(resolved, key=lambda item: (len(item.parts), str(item))):
        if not any(path.is_relative_to(parent) for parent in roots):
            roots.append(path)
    return tuple(roots)


def profile_path(directory: str | Path | ProfilesConfig, name: str) -> Path:
    if not isinstance(name, str) or not _PROFILE_NAME.fullmatch(name):
        raise ValidationError("profile must be a configured profile name, not a path")
    root_value = directory.directory if isinstance(directory, ProfilesConfig) else Path(directory)
    if not root_value.is_absolute():
        raise ValidationError("profiles.directory must be an absolute existing directory")
    try:
        root = root_value.resolve(strict=True)
    except (OSError, RuntimeError) as error:
        raise ValidationError("profiles.directory must be an absolute existing directory") from error
    if not root.is_dir():
        raise ValidationError("profiles.directory must be an absolute existing directory")
    try:
        candidate = (root / f"{name}.md").resolve(strict=True)
    except (OSError, RuntimeError) as error:
        raise ValidationError(f"profile does not exist: {name}") from error
    if not candidate.is_relative_to(root):
        raise PathEscapeError(f"profile escapes configured directory: {name}")
    if not candidate.is_file():
        raise ValidationError(f"profile is not a file: {name}")
    return candidate


def _parse_profile(text: str, name: str) -> tuple[bool, str]:
    allow_write = False
    body = text
    if text.startswith("+++\n"):
        end = text.find("\n+++\n", 4)
        if end < 0:
            raise ValidationError(f"profile {name} has unterminated TOML front matter")
        try:
            metadata = tomllib.loads(text[4:end])
        except tomllib.TOMLDecodeError as error:
            raise ValidationError(f"profile {name} has invalid TOML front matter: {error}") from error
        for key in metadata:
            if key != "write":
                raise ValidationError(f"unknown profile field: profiles.{name}.{key}")
        allow_write = metadata.get("write", False)
        if not isinstance(allow_write, bool):
            raise ValidationError(f"profiles.{name}.write must be a boolean")
        body = text[end + 5 :]
    body = body.strip()
    if not body:
        raise ValidationError(f"profile {name} body must not be blank")
    return allow_write, body


def load_profile(
    directory: str | Path | ProfilesConfig,
    name: str,
    *,
    requested_write: bool = False,
    read_roots: Iterable[str | Path] = (),
) -> AgentProfile:
    """Load a named profile; profile permissions can only narrow a request."""

    if not isinstance(requested_write, bool):
        raise ValidationError("requested_write must be a boolean")
    path = profile_path(directory, name)
    try:
        text = path.read_text(encoding="utf-8")
    except OSError as error:
        raise ValidationError(f"cannot read profile {name}: {error}") from error
    allow_write, body = _parse_profile(text, name)
    return AgentProfile(name, body, requested_write and allow_write, normalize_read_roots(read_roots))
