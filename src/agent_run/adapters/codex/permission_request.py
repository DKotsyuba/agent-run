"""Approve only explicitly trusted MCP namespaces for Codex permission hooks."""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections.abc import Sequence


#: Accepted MCP server identifiers; values become namespace components only.
_SERVER_ID = re.compile(r"^[a-z0-9][a-z0-9_-]*$")


def allows(payload: object, trusted_servers: frozenset[str]) -> bool:
    """Return whether one PermissionRequest targets an explicitly trusted MCP.

    ``payload`` is the decoded Codex hook object and ``trusted_servers`` holds
    validated server identifiers. Missing, malformed, non-PermissionRequest,
    Bash, patch, and unmatched MCP inputs return ``False`` so Codex keeps its
    normal approval flow. The function performs no I/O and grants no shell or
    filesystem authority.
    """

    if not isinstance(payload, dict) or payload.get("hook_event_name") != "PermissionRequest":
        return False
    tool_name = payload.get("tool_name")
    if not isinstance(tool_name, str):
        return False
    return any(
        tool_name.startswith(f"mcp__{variant}__")
        for server in trusted_servers
        for variant in {server, server.replace("-", "_")}
    )


def decision(payload: object, trusted_servers: frozenset[str]) -> dict[str, object] | None:
    """Return Codex's allow response for a trusted MCP request, else ``None``.

    ``payload`` and ``trusted_servers`` follow :func:`allows`. The returned
    mapping is the current PermissionRequest hook contract. ``None`` means the
    hook must stay silent, preserving user or auto-review handling.
    """

    if not allows(payload, trusted_servers):
        return None
    return {
        "hookSpecificOutput": {
            "hookEventName": "PermissionRequest",
            "decision": {"behavior": "allow"},
        }
    }


def parser() -> argparse.ArgumentParser:
    """Return the CLI parser for repeated trusted MCP server declarations."""

    result = argparse.ArgumentParser()
    result.add_argument("--allow-mcp", action="append", required=True)
    return result


def main(argv: Sequence[str] | None = None) -> int:
    """Read one Codex hook event and emit a narrow allow decision when proven.

    ``argv`` contains optional CLI arguments without the executable name; when
    omitted, :mod:`argparse` reads the process arguments. The function reads one
    JSON value from stdin and writes compact JSON only for an allowed MCP call.
    Invalid server identifiers or malformed JSON fail non-zero without echoing
    input, which leaves Codex's ordinary approval path in control.
    """

    arguments = parser().parse_args(argv)
    trusted = frozenset(arguments.allow_mcp)
    if any(_SERVER_ID.fullmatch(server) is None for server in trusted):
        parser().error("--allow-mcp values must be lowercase MCP server identifiers")
    result = decision(json.load(sys.stdin), trusted)
    if result is not None:
        json.dump(result, sys.stdout, separators=(",", ":"))
        sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
