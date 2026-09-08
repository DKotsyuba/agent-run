"""Deterministic evidence for runtime policy enforcement and admission."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import StrEnum

from .errors import ValidationError
from .profiles import AgentProfile


class Constraint(StrEnum):
    """Policy boundaries reported independently by the effective provider."""

    # Removal of the runtime's built-in web search and fetch tools.
    WEB_TOOLS_DISABLED = "web_tools_disabled"
    # Outbound traffic to non-local network addresses.
    EXTERNAL_NETWORK_ISOLATION = "external_network_isolation"
    # TCP traffic to loopback and other host-local listeners.
    LOOPBACK_TCP_ISOLATION = "loopback_tcp_isolation"
    # Access to local Unix-domain sockets.
    UNIX_IPC_ISOLATION = "unix_ipc_isolation"
    # Access to configured MCP transports, distinct from generic Unix IPC.
    MCP_IPC_ISOLATION = "mcp_ipc_isolation"
    # Filesystem writes outside the policy's allowed roots.
    FILESYSTEM_WRITE_ISOLATION = "filesystem_write_isolation"
    # Filesystem reads outside the policy's allowed roots.
    FILESYSTEM_READ_ISOLATION = "filesystem_read_isolation"
    # Immutability of every asset consumed by a configured plugin.
    PLUGIN_IMMUTABILITY = "plugin_immutability"


class Enforcement(StrEnum):
    """The layer that provides one narrowly scoped policy guarantee."""

    # A tool is removed or disabled, without constraining processes or sockets.
    TOOL_FILTER = "tool_filter"
    # The runtime backend actively enforces the stated scope.
    RUNTIME_ENFORCED = "runtime_enforced"
    # The operating system actively enforces the stated scope.
    OS_ENFORCED = "os_enforced"
    # The runtime exposes guidance or configuration without containment.
    ADVISORY = "advisory"
    # No applicable enforcement exists for the stated scope and platform.
    UNSUPPORTED = "unsupported"


@dataclass(frozen=True)
class DeclaredCapability:
    """A runtime's enforcement claim for one constraint.

    ``scope`` states the exact boundary covered, ``reason`` names the backend
    evidence, and an empty ``platforms`` set means the claim applies everywhere.
    The provider does not upgrade this declaration from configuration alone.
    """

    enforcement: Enforcement
    scope: str
    reason: str
    platforms: frozenset[str] = field(default_factory=frozenset)


@dataclass(frozen=True)
class ConstraintEvidence:
    """One constraint's effective level, support bit, and admission status.

    ``constraint`` identifies the boundary and ``enforcement`` names its actual
    layer. ``supported`` says whether that layer satisfies this specific
    boundary; advisory evidence and narrowly scoped tool filtering therefore
    remain visible without satisfying broader isolation constraints.
    ``required`` records the caller's explicit admission requirement. ``scope``,
    ``platform``, and ``reason`` keep the claim bounded and auditable.
    """

    constraint: Constraint
    enforcement: Enforcement
    supported: bool
    required: bool
    scope: str
    platform: str
    reason: str


@dataclass(frozen=True)
class EffectivePolicy:
    """Ordered effective-policy evidence for one runtime and platform.

    ``runtime_name`` and ``platform`` identify the target. ``constraints`` has
    one entry per ``Constraint`` in stable enum order.
    """

    runtime_name: str
    platform: str
    constraints: tuple[ConstraintEvidence, ...]


@dataclass(frozen=True)
class AdmissionDecision:
    """Typed admission result listing only required unsupported constraints.

    ``allowed`` is true exactly when ``unsupported_required`` is empty. Despite
    its stable name, that tuple contains every explicitly required constraint
    whose reported enforcement level does not satisfy the constraint.
    """

    allowed: bool
    unsupported_required: tuple[Constraint, ...]


# Stable evidence order used for deterministic snapshots and admission errors.
_CONSTRAINT_ORDER = tuple(Constraint)
# Conservative reasons used until a backend supplies narrower verified evidence.
_UNSUPPORTED_REASONS = {
    Constraint.EXTERNAL_NETWORK_ISOLATION: (
        "tool filtering does not prevent shell or child-process external sockets"
    ),
    Constraint.LOOPBACK_TCP_ISOLATION: (
        "no backend declared isolation from loopback or host-local TCP listeners"
    ),
    Constraint.UNIX_IPC_ISOLATION: "no backend declared Unix-domain socket isolation",
    Constraint.MCP_IPC_ISOLATION: "no backend declared isolation from configured MCP transports",
    Constraint.FILESYSTEM_WRITE_ISOLATION: (
        "profile write grants and runtime HOME do not provide OS write containment"
    ),
    Constraint.FILESYSTEM_READ_ISOLATION: (
        "profile read roots and runtime HOME do not provide OS read containment"
    ),
    Constraint.PLUGIN_IMMUTABILITY: (
        "the runtime did not declare validated materialization of all selected plugin assets"
    ),
}


def _unsupported(constraint: Constraint, platform: str, reason: str, required: bool) -> ConstraintEvidence:
    """Build unsupported evidence for one exact boundary without broad claims."""

    return ConstraintEvidence(
        constraint=constraint,
        enforcement=Enforcement.UNSUPPORTED,
        supported=False,
        required=required,
        scope=constraint.value,
        platform=platform,
        reason=reason,
    )


def _satisfies(constraint: Constraint, enforcement: Enforcement) -> bool:
    """Return whether ``enforcement`` is strong enough for ``constraint``."""

    if enforcement in {Enforcement.ADVISORY, Enforcement.UNSUPPORTED}:
        return False
    return (
        enforcement is not Enforcement.TOOL_FILTER
        or constraint is Constraint.WEB_TOOLS_DISABLED
    )


def effective_policy(
    profile: AgentProfile,
    runtime_name: str,
    platform: str,
    capabilities: Mapping[Constraint, DeclaredCapability],
    *,
    required: frozenset[Constraint] = frozenset(),
) -> EffectivePolicy:
    """Resolve effective policy evidence without changing runtime permissions.

    ``profile`` is the already-effective legacy profile, ``runtime_name`` and
    ``platform`` identify the evaluated target, and ``capabilities`` contains
    backend claims whose scopes have been independently established. ``required``
    is explicit: profile grants never imply it. Invalid typed inputs raise
    ``ValidationError`` without echoing their values. Every known constraint is
    returned once in stable enum order.
    """

    if not isinstance(profile, AgentProfile):
        raise ValidationError("profile must be an AgentProfile")
    if not isinstance(runtime_name, str) or not runtime_name.strip():
        raise ValidationError("runtime_name must be a nonblank string")
    if not isinstance(platform, str) or not platform.strip():
        raise ValidationError("platform must be a nonblank string")
    if not isinstance(capabilities, Mapping):
        raise ValidationError("capabilities must map Constraint to DeclaredCapability")
    if not isinstance(required, frozenset) or any(
        not isinstance(item, Constraint) for item in required
    ):
        raise ValidationError("required must be a frozenset of Constraint values")
    for constraint, capability in capabilities.items():
        if not isinstance(constraint, Constraint) or not isinstance(
            capability, DeclaredCapability
        ):
            raise ValidationError("capabilities must map Constraint to DeclaredCapability")
        if (
            not isinstance(capability.enforcement, Enforcement)
            or not isinstance(capability.scope, str)
            or not capability.scope.strip()
            or not isinstance(capability.reason, str)
            or not capability.reason.strip()
            or not isinstance(capability.platforms, frozenset)
            or any(not isinstance(item, str) or not item.strip() for item in capability.platforms)
        ):
            raise ValidationError("capabilities contain an invalid declaration")

    evidence: list[ConstraintEvidence] = []
    for constraint in _CONSTRAINT_ORDER:
        is_required = constraint in required
        capability = capabilities.get(constraint)
        if capability is not None:
            if capability.platforms and platform not in capability.platforms:
                supported_platforms = ", ".join(sorted(capability.platforms))
                evidence.append(
                    _unsupported(
                        constraint,
                        platform,
                        f"declared only for platforms: {supported_platforms}",
                        is_required,
                    )
                )
            else:
                evidence.append(
                    ConstraintEvidence(
                        constraint=constraint,
                        enforcement=capability.enforcement,
                        supported=_satisfies(constraint, capability.enforcement),
                        required=is_required,
                        scope=capability.scope,
                        platform=platform,
                        reason=capability.reason,
                    )
                )
            continue

        if constraint is Constraint.WEB_TOOLS_DISABLED and not profile.network:
            evidence.append(
                ConstraintEvidence(
                    constraint=constraint,
                    enforcement=Enforcement.TOOL_FILTER,
                    supported=True,
                    required=is_required,
                    scope="runtime WebFetch and WebSearch tools only",
                    platform=platform,
                    reason="the effective profile removes built-in web fetch and search tools",
                )
            )
        elif constraint is Constraint.WEB_TOOLS_DISABLED:
            evidence.append(
                _unsupported(
                    constraint,
                    platform,
                    "the effective profile authorizes built-in web tools",
                    is_required,
                )
            )
        else:
            evidence.append(
                _unsupported(
                    constraint,
                    platform,
                    _UNSUPPORTED_REASONS[constraint],
                    is_required,
                )
            )
    return EffectivePolicy(runtime_name, platform, tuple(evidence))


def admission_decision(policy: EffectivePolicy) -> AdmissionDecision:
    """Allow unless an explicit requirement lacks sufficient enforcement."""

    if not isinstance(policy, EffectivePolicy):
        raise ValidationError("policy must be an EffectivePolicy")
    unsupported = tuple(
        item.constraint
        for item in policy.constraints
        if item.required and not item.supported
    )
    return AdmissionDecision(not unsupported, unsupported)
