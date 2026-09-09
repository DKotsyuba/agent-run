"""Runtime-neutral resolution of canonical role contracts."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Mapping, cast

from .adapters.home import content_hash
from .adapters.snapshot_tree import tree_revision
from .config import McpConfig
from .domain import Constraint
from .errors import ValidationError
from .profiles import AgentProfile, normalize_read_roots


_CATALOG_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]*\Z")
_ACCOUNT_ID = re.compile(r"[a-z0-9_-]{1,32}\Z")
_ENV_NAME = re.compile(r"[A-Z_][A-Z0-9_]*\Z")
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")


def _object(value: object, keys: frozenset[str], path: str) -> dict[str, object]:
    """Return an exact string-keyed object or reject its shape."""

    if type(value) is not dict or set(value) != keys:
        raise ValidationError(f"{path} has an invalid shape")
    return value


def _text(value: object, path: str, *, blank: bool = False) -> str:
    """Return one JSON string without NUL, optionally allowing blank."""

    if not isinstance(value, str) or "\0" in value or (not blank and not value.strip()):
        raise ValidationError(f"{path} must be a {'string' if blank else 'nonblank string'}")
    return value


def _strings(
    value: object, path: str, *, unique: bool = True
) -> tuple[str, ...]:
    """Return NUL-free JSON strings, rejecting duplicates when requested."""

    if not isinstance(value, list):
        raise ValidationError(f"{path} must be a list of strings")
    items = tuple(_text(item, f"{path}[]", blank=True) for item in value)
    if unique and len(set(items)) != len(items):
        raise ValidationError(f"{path} must not contain duplicates")
    return items


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

        return {**_canonical_payload(self), "config_revision": self.config_revision}

    @classmethod
    def from_payload(cls, payload: object) -> ResolvedRolePlan:
        """Validate and reconstruct one detached JSON role payload.

        Exact object shapes and JSON scalar types are required. Paths must be
        existing absolute directories in normalized antichain order; IDs,
        hashes, MCP transport/environment declarations, constraints, and auth
        choice are validated. The credential-free config revision is recomputed
        before the immutable plan is returned.
        """

        document = _object(
            payload,
            frozenset(
                {
                    "role_name", "role_revision", "prompt", "grants", "skills",
                    "mcp", "required_constraints", "auth", "config_revision",
                }
            ),
            "resolved role",
        )
        grants = _object(
            document["grants"],
            frozenset(
                {"write", "network", "allow_external_read_roots", "read_roots"}
            ),
            "resolved role grants",
        )
        for name in ("write", "network", "allow_external_read_roots"):
            if type(grants[name]) is not bool:
                raise ValidationError(f"resolved role grants.{name} must be a boolean")
        write = cast(bool, grants["write"])
        network = cast(bool, grants["network"])
        allow_external = cast(bool, grants["allow_external_read_roots"])
        root_values = _strings(grants["read_roots"], "resolved role grants.read_roots")
        roots = normalize_read_roots(root_values)
        if tuple(str(root) for root in roots) != root_values:
            raise ValidationError("resolved role read_roots must be a normalized antichain")
        if roots and not allow_external:
            raise ValidationError("resolved role does not allow external read roots")

        raw_skills = document["skills"]
        if not isinstance(raw_skills, list):
            raise ValidationError("resolved role skills must be a list")
        skills = []
        for index, value in enumerate(raw_skills):
            item = _object(value, frozenset({"id", "revision"}), f"resolved role skills[{index}]")
            skill_id = _text(item["id"], f"resolved role skills[{index}].id")
            revision = _text(item["revision"], f"resolved role skills[{index}].revision")
            if _CATALOG_ID.fullmatch(skill_id) is None or _SHA256.fullmatch(revision) is None:
                raise ValidationError(f"resolved role skills[{index}] is invalid")
            skills.append(ResolvedSkill(skill_id, revision))
        if len({skill.id for skill in skills}) != len(skills):
            raise ValidationError("resolved role skill ids must not contain duplicates")

        raw_mcp = document["mcp"]
        if not isinstance(raw_mcp, list):
            raise ValidationError("resolved role mcp must be a list")
        servers = []
        for index, value in enumerate(raw_mcp):
            item = _object(
                value,
                frozenset({"id", "transport", "command", "args", "env_from"}),
                f"resolved role mcp[{index}]",
            )
            server_id = _text(item["id"], f"resolved role mcp[{index}].id")
            transport = _text(item["transport"], f"resolved role mcp[{index}].transport")
            command = _text(item["command"], f"resolved role mcp[{index}].command")
            args = _strings(
                item["args"], f"resolved role mcp[{index}].args", unique=False
            )
            env_from = _strings(item["env_from"], f"resolved role mcp[{index}].env_from")
            try:
                canonical_command = str(Path(command).expanduser().resolve())
            except (OSError, RuntimeError) as error:
                raise ValidationError(
                    f"resolved role mcp[{index}].command is invalid"
                ) from error
            if (
                _CATALOG_ID.fullmatch(server_id) is None
                or transport != "stdio"
                or command != canonical_command
                or any(_ENV_NAME.fullmatch(name) is None for name in env_from)
            ):
                raise ValidationError(f"resolved role mcp[{index}] is invalid")
            servers.append(ResolvedMcp(server_id, transport, command, args, env_from))
        if len({server.id for server in servers}) != len(servers):
            raise ValidationError("resolved role MCP ids must not contain duplicates")

        constraint_values = _strings(
            document["required_constraints"], "resolved role required_constraints"
        )
        try:
            constraints = frozenset(Constraint(value) for value in constraint_values)
        except ValueError as error:
            raise ValidationError("resolved role contains an unknown constraint") from error
        if tuple(sorted(item.value for item in constraints)) != constraint_values:
            raise ValidationError("resolved role constraints must be sorted and unique")

        auth = _object(
            document["auth"], frozenset({"mode", "reference"}), "resolved role auth"
        )
        auth_mode = _text(auth["mode"], "resolved role auth.mode")
        auth_reference = auth["reference"]
        if auth_reference is not None:
            auth_reference = _text(auth_reference, "resolved role auth.reference")
        if auth_mode not in {"global", "account"} or (
            auth_mode == "account"
        ) != (auth_reference is not None):
            raise ValidationError("resolved role auth choice is invalid")
        if auth_reference is not None and _ACCOUNT_ID.fullmatch(auth_reference) is None:
            raise ValidationError("resolved role auth reference is invalid")

        role_name = _text(document["role_name"], "resolved role role_name")
        role_revision = _text(document["role_revision"], "resolved role role_revision")
        prompt = _text(document["prompt"], "resolved role prompt")
        config_revision = _text(document["config_revision"], "resolved role config_revision")
        if _CATALOG_ID.fullmatch(role_name) is None or _SHA256.fullmatch(config_revision) is None:
            raise ValidationError("resolved role identity or config revision is invalid")
        seed = {key: value for key, value in document.items() if key != "config_revision"}
        expected = content_hash(json.dumps(seed, sort_keys=True, separators=(",", ":")))
        if config_revision != expected:
            raise ValidationError("resolved role config revision does not match its payload")
        plan = cls(
            role_name, role_revision, prompt, write, network,
            allow_external, roots, tuple(skills), tuple(servers),
            constraints, auth_mode, auth_reference, config_revision,
        )
        if plan.to_payload() != document:
            raise ValidationError("resolved role payload is not canonical")
        return plan


def _canonical_payload(plan: ResolvedRolePlan) -> dict[str, object]:
    """Build the one canonical role payload excluding its derived revision."""

    return {
        "role_name": plan.role_name,
        "role_revision": plan.role_revision,
        "prompt": plan.prompt,
        "grants": {
            "write": plan.write,
            "network": plan.network,
            "allow_external_read_roots": plan.allow_external_read_roots,
            "read_roots": [str(path) for path in plan.read_roots],
        },
        "skills": [
            {"id": skill.id, "revision": skill.revision}
            for skill in plan.skills
        ],
        "mcp": [
            {
                "id": server.id,
                "transport": server.transport,
                "command": server.command,
                "args": list(server.args),
                "env_from": list(server.env_from),
            }
            for server in plan.mcp
        ],
        "required_constraints": sorted(
            constraint.value for constraint in plan.required_constraints
        ),
        "auth": {"mode": plan.auth_mode, "reference": plan.auth_reference},
    }

def resolve_role_plan(
    profile: AgentProfile,
    *,
    skills_root: Path,
    mcp_catalog: Mapping[str, McpConfig],
    auth_mode: str = "global",
    auth_reference: str | None = None,
    skill_revision_cache: dict[Path, str] | None = None,
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
        revision = None if skill_revision_cache is None else skill_revision_cache.get(source)
        if revision is None:
            revision = tree_revision(source)
            if skill_revision_cache is not None:
                skill_revision_cache[source] = revision
        skills.append(ResolvedSkill(name, revision))

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
    plan = ResolvedRolePlan(
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
        "",
    )
    revision = content_hash(
        json.dumps(_canonical_payload(plan), sort_keys=True, separators=(",", ":"))
    )
    return replace(plan, config_revision=revision)
