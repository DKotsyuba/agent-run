"""Contract tests for honest effective-policy evidence and admission."""

from pathlib import Path

import pytest

from agent_run.effective_policy import (
    Constraint,
    DeclaredCapability,
    Enforcement,
    admission_decision,
    effective_policy,
)
from agent_run.errors import ValidationError
from agent_run.profiles import AgentProfile


def _profile(*, network: bool = False) -> AgentProfile:
    """Return a minimal effective legacy profile for policy tests."""

    return AgentProfile("implement", "role", False, (Path("/tmp"),), network)


def test_network_boundaries_are_independent_and_legacy_admission_stays_open() -> None:
    """Tool removal never claims process, loopback, Unix, or MCP isolation."""

    policy = effective_policy(_profile(), "claude", "darwin", {})
    by_constraint = {item.constraint: item for item in policy.constraints}

    assert by_constraint[Constraint.WEB_TOOLS_DISABLED].enforcement is Enforcement.TOOL_FILTER
    assert by_constraint[Constraint.WEB_TOOLS_DISABLED].supported is True
    for constraint in (
        Constraint.EXTERNAL_NETWORK_ISOLATION,
        Constraint.LOOPBACK_TCP_ISOLATION,
        Constraint.UNIX_IPC_ISOLATION,
        Constraint.MCP_IPC_ISOLATION,
    ):
        assert by_constraint[constraint].enforcement is Enforcement.UNSUPPORTED
        assert by_constraint[constraint].supported is False
        assert by_constraint[constraint].platform == "darwin"
        assert by_constraint[constraint].reason
    assert admission_decision(policy).allowed is True


def test_only_explicit_required_unsupported_constraints_reject() -> None:
    """A runtime-enforced plugin snapshot passes while missing OS isolation rejects."""

    required = frozenset(
        {Constraint.PLUGIN_IMMUTABILITY, Constraint.EXTERNAL_NETWORK_ISOLATION}
    )
    policy = effective_policy(
        _profile(),
        "claude",
        "darwin",
        {
            Constraint.PLUGIN_IMMUTABILITY: DeclaredCapability(
                Enforcement.RUNTIME_ENFORCED,
                "declared plugin assets copied into an immutable attempt snapshot",
                "materialization validated containment and rejected symlinks",
            )
        },
        required=required,
    )

    plugin = next(
        item for item in policy.constraints if item.constraint is Constraint.PLUGIN_IMMUTABILITY
    )
    decision = admission_decision(policy)
    assert plugin.supported is True
    assert plugin.required is True
    assert decision.allowed is False
    assert decision.unsupported_required == (Constraint.EXTERNAL_NETWORK_ISOLATION,)


def test_platform_scopes_are_applied_without_upgrading_configuration() -> None:
    """A backend claim for another platform remains explicitly unsupported."""

    policy = effective_policy(
        _profile(network=True),
        "codex",
        "darwin",
        {
            Constraint.FILESYSTEM_WRITE_ISOLATION: DeclaredCapability(
                Enforcement.OS_ENFORCED,
                "writes outside selected roots",
                "Linux sandbox probe passed",
                frozenset({"linux"}),
            )
        },
        required=frozenset({Constraint.FILESYSTEM_WRITE_ISOLATION}),
    )

    write = next(
        item
        for item in policy.constraints
        if item.constraint is Constraint.FILESYSTEM_WRITE_ISOLATION
    )
    assert write.enforcement is Enforcement.UNSUPPORTED
    assert write.platform == "darwin"
    assert "linux" in write.reason
    assert admission_decision(policy).unsupported_required == (
        Constraint.FILESYSTEM_WRITE_ISOLATION,
    )


@pytest.mark.parametrize("required", [set(), (Constraint.PLUGIN_IMMUTABILITY,)])
def test_required_input_is_strictly_typed(required: object) -> None:
    """Mutable sets and tuples cannot silently become admission requirements."""

    with pytest.raises(ValidationError, match="frozenset"):
        effective_policy(_profile(), "claude", "darwin", {}, required=required)  # type: ignore[arg-type]
