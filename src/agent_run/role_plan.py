"""Runtime-neutral resolution of canonical role contracts."""

from __future__ import annotations

import json
import hashlib
import re
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Mapping

from .config import McpConfig
from .domain import Constraint
from .errors import ValidationError
from .profiles import AgentProfile, normalize_read_roots


_CATALOG_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]*\Z")


def _content_hash(value: str | bytes) -> str:
    """Return a SHA-256 hex digest for canonical role bytes."""

    payload = value.encode("utf-8") if isinstance(value, str) else value
    return hashlib.sha256(payload).hexdigest()


def _skill_revision(source: Path) -> str:
    """Hash one canonical real skill tree without following links."""

    if source.is_symlink() or not source.is_dir():
        raise ValidationError(f"canonical skill must be a real directory: {source.name}")
    entries = []
    for path in sorted(source.rglob("*")):
        relative = path.relative_to(source).as_posix()
        if path.is_symlink():
            raise ValidationError(f"canonical skill contains a symlink: {source.name}/{relative}")
        if path.is_dir():
            entries.append([relative, "directory"])
        elif path.is_file():
            payload = path.read_bytes()
            entries.append([relative, "file", len(payload), _content_hash(payload)])
        else:
            raise ValidationError(f"canonical skill contains a special file: {source.name}/{relative}")
    return _content_hash(json.dumps(entries, separators=(",", ":")))


@dataclass(frozen=True, slots=True)
class ResolvedSkill:
    """One canonical skill identity and content revision."""

    id: str
    revision: str


@dataclass(frozen=True, slots=True)
class ResolvedMcp:
    """One credential-free MCP definition selected by a role."""

    id: str
    transport: str
    command: str
    args: tuple[str, ...]
    env_from: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class ResolvedRolePlan:
    """Immutable, serializable role contract shared by every adapter.

    The plan contains the complete prompt and effective grants, selected skill
    and MCP revisions, task read roots, and an auth choice/reference. It never
    contains credential bytes, argv, process environment, generated-home paths,
    or an adapter ``LaunchPlan``.
    """

    role_name: str
    role_revision: str
    prompt: str
    write: bool
    network: bool
    allow_external_read_roots: bool
    read_roots: tuple[Path, ...]
    skills: tuple[ResolvedSkill, ...]
    mcp: tuple[ResolvedMcp, ...]
    required_constraints: frozenset[Constraint]
    auth_mode: str
    auth_reference: str | None
    config_revision: str

    def to_payload(self) -> dict[str, object]:
        """Return the canonical JSON-safe role document without live secrets."""

        return {
            "role_name": self.role_name,
            "role_revision": self.role_revision,
            "prompt": self.prompt,
            "grants": {
                "write": self.write,
                "network": self.network,
                "allow_external_read_roots": self.allow_external_read_roots,
                "read_roots": [str(path) for path in self.read_roots],
            },
            "skills": [
                {"id": skill.id, "revision": skill.revision}
                for skill in self.skills
            ],
            "mcp": [
                {
                    "id": server.id,
                    "transport": server.transport,
                    "command": server.command,
                    "args": list(server.args),
                    "env_from": list(server.env_from),
                }
                for server in self.mcp
            ],
            "required_constraints": sorted(
                constraint.value for constraint in self.required_constraints
            ),
            "auth": {
                "mode": self.auth_mode,
                "reference": self.auth_reference,
            },
            "config_revision": self.config_revision,
        }

    def to_profile(self) -> AgentProfile:
        """Return the compatibility profile consumed by native translators."""

        return AgentProfile(
            name=self.role_name,
            body=self.prompt,
            write=self.write,
            read_roots=self.read_roots,
            network=self.network,
            revision=self.role_revision,
            allow_external_read_roots=self.allow_external_read_roots,
            skills=tuple(skill.id for skill in self.skills),
            mcp=tuple(server.id for server in self.mcp),
            required_constraints=self.required_constraints,
            canonical=True,
        )

    def mcp_configs(self) -> Mapping[str, McpConfig]:
        """Return immutable typed MCP definitions for adapter translation."""

        return MappingProxyType(
            {
                server.id: McpConfig(
                    server.transport,
                    Path(server.command),
                    server.args,
                    server.env_from,
                )
                for server in self.mcp
            }
        )


def resolve_role_plan(
    profile: AgentProfile,
    *,
    skills_root: Path,
    mcp_catalog: Mapping[str, McpConfig],
    auth_mode: str = "global",
    auth_reference: str | None = None,
) -> ResolvedRolePlan:
    """Resolve one canonical profile against shared skill and MCP catalogs.

    Missing or unsafe skills, missing MCP definitions, disallowed task read
    roots, and inconsistent auth choices raise ``ValidationError``. The
    returned revision hashes the full credential-free payload, so identical
    inputs produce identical role plans for every runtime.
    """

    if not isinstance(profile, AgentProfile) or not profile.canonical:
        raise ValidationError("resolved role plans require a canonical profile")
    if not isinstance(skills_root, Path) or not skills_root.is_absolute():
        raise ValidationError("skills.directory must be absolute")
    if profile.read_roots and not profile.allow_external_read_roots:
        raise ValidationError(
            f"profile {profile.name} does not allow external read roots"
        )
    if auth_mode not in {"global", "account"}:
        raise ValidationError("role auth mode must be 'global' or 'account'")
    if (auth_mode == "account") != (auth_reference is not None):
        raise ValidationError("account auth requires exactly one non-secret reference")

    skills = []
    for name in profile.skills:
        if _CATALOG_ID.fullmatch(name) is None:
            raise ValidationError(f"invalid canonical skill id: {name}")
        source = skills_root / name
        try:
            source.relative_to(skills_root)
        except ValueError as error:
            raise ValidationError(f"skill escapes canonical catalog: {name}") from error
        if not (source / "SKILL.md").is_file():
            raise ValidationError(f"canonical skill is not available: {name}")
        skills.append(ResolvedSkill(name, _skill_revision(source)))

    servers = []
    for name in profile.mcp:
        if _CATALOG_ID.fullmatch(name) is None:
            raise ValidationError(f"invalid role MCP id: {name}")
        try:
            definition = mcp_catalog[name]
        except KeyError as error:
            raise ValidationError(f"role references unknown MCP server: {name}") from error
        servers.append(
            ResolvedMcp(
                name,
                definition.transport,
                str(definition.command),
                definition.args,
                definition.env_from,
            )
        )

    roots = normalize_read_roots(profile.read_roots)
    seed = {
        "role_name": profile.name,
        "role_revision": profile.revision,
        "prompt": profile.body,
        "grants": {
            "write": profile.write,
            "network": profile.network,
            "allow_external_read_roots": profile.allow_external_read_roots,
            "read_roots": [str(path) for path in roots],
        },
        "skills": [
            {"id": skill.id, "revision": skill.revision} for skill in skills
        ],
        "mcp": [
            {
                "id": server.id,
                "transport": server.transport,
                "command": server.command,
                "args": list(server.args),
                "env_from": list(server.env_from),
            }
            for server in servers
        ],
        "required_constraints": sorted(
            constraint.value for constraint in profile.required_constraints
        ),
        "auth": {"mode": auth_mode, "reference": auth_reference},
    }
    revision = _content_hash(
        json.dumps(seed, sort_keys=True, separators=(",", ":"))
    )
    return ResolvedRolePlan(
        profile.name,
        profile.revision,
        profile.body,
        profile.write,
        profile.network,
        profile.allow_external_read_roots,
        roots,
        tuple(skills),
        tuple(servers),
        profile.required_constraints,
        auth_mode,
        auth_reference,
        revision,
    )
