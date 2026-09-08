"""Small constructors for direct resolved-role adapter tests."""

from __future__ import annotations

from agent_run.config import McpConfig, RuntimeConfig
from agent_run.domain import StartRequest
from agent_run.errors import ValidationError
from agent_run.profiles import AgentProfile
from agent_run.role_plan import ResolvedMcp, ResolvedRolePlan, ResolvedSkill


def resolved_role(
    request: StartRequest,
    profile: AgentProfile,
    config: RuntimeConfig,
    mcp_servers: dict[str, McpConfig],
) -> ResolvedRolePlan:
    """Build a deterministic secret-free role matching one direct test request."""

    if not isinstance(mcp_servers, dict):
        raise ValidationError("test role requires a resolved mcp_servers mapping")
    servers = []
    for name in config.mcp:
        server = mcp_servers.get(name)
        if server is None:
            raise ValidationError(f"no resolved MCP definition for test role: {name}")
        servers.append(
            ResolvedMcp(
                name, server.transport, str(server.command), server.args, server.env_from
            )
        )
    return ResolvedRolePlan(
        profile.name,
        profile.revision,
        profile.body,
        request.write and profile.write,
        profile.network,
        profile.allow_external_read_roots,
        request.read_roots,
        tuple(ResolvedSkill(name, "a" * 64) for name in config.skills),
        tuple(servers),
        profile.required_constraints,
        "global",
        None,
        "b" * 64,
    )
