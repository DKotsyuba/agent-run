"""Named role profiles and their effective permissions."""

from __future__ import annotations

import re
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Iterable

from .config import ProfilesConfig
from .errors import PathEscapeError, ValidationError

if TYPE_CHECKING:
    from .effective_policy import Constraint


_PROFILE_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]*\Z")


@dataclass(frozen=True)
class AgentProfile:
    """One loaded legacy profile or complete canonical role contract.

    ``canonical`` distinguishes explicit revisioned roles from compatibility
    profiles. Canonical roles own their write/network grants, external read-root
    rule, skills, MCP servers, and required enforcement constraints. Legacy
    profiles retain request-narrowed write behavior until configuration
    migration.
    """

    name: str
    body: str
    write: bool
    read_roots: tuple[Path, ...] = ()
    network: bool = False
    revision: str = "legacy"
    allow_external_read_roots: bool = True
    skills: tuple[str, ...] = ()
    mcp: tuple[str, ...] = ()
    required_constraints: frozenset[Constraint] = frozenset()
    canonical: bool = False


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


def _names(value: object, path: str) -> tuple[str, ...]:
    """Return unique nonblank string names from one profile list."""

    if not isinstance(value, list) or any(
        not isinstance(item, str) or not item.strip() for item in value
    ):
        raise ValidationError(f"{path} must be a list of nonblank strings")
    names = tuple(value)
    if len(set(names)) != len(names):
        raise ValidationError(f"{path} must not contain duplicates")
    return names


def _parse_profile(
    text: str,
    name: str,
    *,
    requested_write: bool,
    read_roots: Iterable[str | Path],
) -> AgentProfile:
    """Parse one legacy profile or complete revisioned role contract."""

    allow_write = False
    allow_network = False
    canonical = False
    metadata: dict[str, object] = {}
    body = text
    if text.startswith("+++\n"):
        end = text.find("\n+++\n", 4)
        if end < 0:
            raise ValidationError(f"profile {name} has unterminated TOML front matter")
        try:
            metadata = tomllib.loads(text[4:end])
        except tomllib.TOMLDecodeError as error:
            raise ValidationError(f"profile {name} has invalid TOML front matter: {error}") from error
        canonical_fields = {
            "revision",
            "write",
            "network",
            "allow_external_read_roots",
            "skills",
            "mcp",
            "required_constraints",
        }
        for key in metadata:
            if key not in canonical_fields:
                raise ValidationError(f"unknown profile field: profiles.{name}.{key}")
        canonical = "revision" in metadata
        if not canonical and set(metadata) - {"write", "network"}:
            raise ValidationError(
                f"profile {name} canonical fields require a revision"
            )
        if canonical and set(metadata) != canonical_fields:
            missing = ", ".join(sorted(canonical_fields - set(metadata)))
            raise ValidationError(
                f"profile {name} canonical role is incomplete; missing: {missing}"
            )
        allow_write = metadata.get("write", False)
        if not isinstance(allow_write, bool):
            raise ValidationError(f"profiles.{name}.write must be a boolean")
        allow_network = metadata.get("network", False)
        if not isinstance(allow_network, bool):
            raise ValidationError(f"profiles.{name}.network must be a boolean")
        body = text[end + 5 :]
    body = body.strip()
    if not body:
        raise ValidationError(f"profile {name} body must not be blank")
    roots = normalize_read_roots(read_roots)
    if not canonical:
        return AgentProfile(
            name,
            body,
            requested_write and allow_write,
            roots,
            allow_network,
        )
    revision = metadata["revision"]
    if not isinstance(revision, str) or not revision.strip():
        raise ValidationError(f"profiles.{name}.revision must be a nonblank string")
    allow_external = metadata["allow_external_read_roots"]
    if not isinstance(allow_external, bool):
        raise ValidationError(
            f"profiles.{name}.allow_external_read_roots must be a boolean"
        )
    if roots and not allow_external:
        raise ValidationError(f"profile {name} does not allow external read roots")
    required_names = _names(
        metadata["required_constraints"],
        f"profiles.{name}.required_constraints",
    )
    from .effective_policy import Constraint

    try:
        required = frozenset(Constraint(item) for item in required_names)
    except ValueError as error:
        raise ValidationError(
            f"profiles.{name}.required_constraints contains an unknown constraint"
        ) from error
    return AgentProfile(
        name=name,
        body=body,
        write=allow_write,
        read_roots=roots,
        network=allow_network,
        revision=revision,
        allow_external_read_roots=allow_external,
        skills=_names(metadata["skills"], f"profiles.{name}.skills"),
        mcp=_names(metadata["mcp"], f"profiles.{name}.mcp"),
        required_constraints=required,
        canonical=True,
    )


def load_profile(
    directory: str | Path | ProfilesConfig,
    name: str,
    *,
    requested_write: bool = False,
    read_roots: Iterable[str | Path] = (),
) -> AgentProfile:
    """Load one named legacy profile or complete revisioned role.

    ``requested_write`` narrows legacy profiles only. A canonical role owns its
    effective write grant; requested task read roots are accepted only when its
    explicit external-root rule allows them.
    """

    if not isinstance(requested_write, bool):
        raise ValidationError("requested_write must be a boolean")
    path = profile_path(directory, name)
    try:
        text = path.read_text(encoding="utf-8")
    except OSError as error:
        raise ValidationError(f"cannot read profile {name}: {error}") from error
    return _parse_profile(
        text,
        name,
        requested_write=requested_write,
        read_roots=read_roots,
    )
