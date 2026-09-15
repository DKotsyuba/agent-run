"""Declarative native runtime settings: validation, ownership, conversion.

``runtimes.<name>.native_settings`` in the common agent-run config declares
tuning values that adapters merge into each native runtime's own generated
preference file (Codex ``config.toml``, Claude/GLM ``settings.json``, Qwen
``.qwen/settings.json``) so an operator can retune a runtime without editing
adapter Python or reinstalling the package. This module owns the shared
contract: strict value types, key hygiene, immutable storage, deterministic
JSON conversion, and the reserved control namespaces that stay owned by
agent-run or by the native runtime's security model and therefore fail
closed instead of being silently overwritten.
"""

from __future__ import annotations

import math
import re
from types import MappingProxyType
from typing import Mapping

from .errors import ValidationError

__all__ = [
    "CODEX_RESERVED_ROOTS",
    "CLAUDE_RESERVED_ROOTS",
    "QWEN_RESERVED_ROOTS",
    "ADAPTER_RESERVED_ROOTS",
    "validate_native_settings",
    "enforce_native_settings",
    "native_settings_json",
]

#: Section keys must be plain identifiers. Dots are rejected so a literal
#: dotted key can never splice itself into an unrelated namespace when the
#: tree is rendered back into TOML sections or JSON objects.
_SETTING_KEY = re.compile(r"[A-Za-z0-9_-]+\Z")

#: Codex ``config.toml`` roots that agent-run or the launch contract owns:
#: request/role model and reasoning selection, approval and sandbox policy,
#: credential and environment plumbing (``shell_environment_policy`` can
#: inject or inherit environment), executable command surfaces (``notify``),
#: feature/hook control, telemetry, profiles, and every generated table
#: (projects/permissions/plugins/hooks/mcp_servers). Unknown benign tuning
#: keys outside this set remain declarable.
CODEX_RESERVED_ROOTS = frozenset(
    {
        "model",
        "model_provider",
        "model_providers",
        "model_reasoning_effort",
        "openai_base_url",
        "chatgpt_base_url",
        "openai_api_key",
        "approvals_reviewer",
        "approval_policy",
        "sandbox_mode",
        "sandbox_workspace_write",
        "shell_environment_policy",
        "notify",
        "features",
        "otel",
        "profile",
        "profiles",
        "projects",
        "default_permissions",
        "permissions",
        "plugins",
        "hooks",
        "mcp_servers",
        "trust",
        "auth",
        "credentials",
        "env",
        "environment",
        "provider",
        "providers",
        "web",
    }
)

#: Claude/GLM ``settings.json`` roots that stay owned: generated hooks and
#: MCP routing, environment and credential surfaces (``apiKeyHelper`` is an
#: executable command), auth forcing, isolation/permission controls, hook
#: disablement and other command surfaces (``statusLine``), MCP enablement
#: scopes, and request-owned model selection.
CLAUDE_RESERVED_ROOTS = frozenset(
    {
        "model",
        "env",
        "environment",
        "permissions",
        "sandbox",
        "credentials",
        "auth",
        "hooks",
        "mcpServers",
        "apiKeyHelper",
        "forceLoginMethod",
        "forceLoginOrgUUID",
        "disableAllHooks",
        "statusLine",
        "enableAllProjectMcpServers",
        "enabledMcpjsonServers",
        "disabledMcpjsonServers",
        "awsAuthRefresh",
        "awsCredentialExport",
        "gcpAuthRefresh",
        "enabledPlugins",
        "extraKnownMarketplaces",
        "fileSuggestion",
        "providers",
    }
)

#: Qwen ``.qwen/settings.json`` roots that stay owned: ``tools`` carries the
#: non-negotiable ``sandbox = true`` isolation control, ``context`` points at
#: the generated context file, and ``security``/``permissions``/``hooks``/
#: ``mcpServers``/``skills`` define granted capabilities and auth routing.
QWEN_RESERVED_ROOTS = frozenset(
    {
        "model",
        "env",
        "environment",
        "credentials",
        "auth",
        "tools",
        "context",
        "security",
        "permissions",
        "hooks",
        "mcpServers",
        "mcp",
        "skills",
        "sandbox",
    }
)

#: Adapter import refs (both ``module:ADAPTER`` spellings) whose native
#: settings pass through a packaged adapter with a reserved-root contract.
#: Other adapters fail closed: they have no merge implementation, so a
#: declared table would be silently ignored.
ADAPTER_RESERVED_ROOTS: Mapping[str, frozenset[str]] = MappingProxyType(
    {
        "agent_run.adapters.codex:ADAPTER": CODEX_RESERVED_ROOTS,
        "agent_run.adapters.codex.adapter:ADAPTER": CODEX_RESERVED_ROOTS,
        "agent_run.adapters.claude:ADAPTER": CLAUDE_RESERVED_ROOTS,
        "agent_run.adapters.claude.adapter:ADAPTER": CLAUDE_RESERVED_ROOTS,
        "agent_run.adapters.glm:ADAPTER": CLAUDE_RESERVED_ROOTS,
        "agent_run.adapters.glm.adapter:ADAPTER": CLAUDE_RESERVED_ROOTS,
        "agent_run.adapters.qwen:ADAPTER": QWEN_RESERVED_ROOTS,
        "agent_run.adapters.qwen.adapter:ADAPTER": QWEN_RESERVED_ROOTS,
    }
)

#: Human-readable adapter names for ownership errors, keyed by reserved set.
_RESERVED_LABELS = {
    id(CODEX_RESERVED_ROOTS): "codex",
    id(CLAUDE_RESERVED_ROOTS): "claude/glm",
    id(QWEN_RESERVED_ROOTS): "qwen",
}


def validate_native_settings(value: object, path: str) -> Mapping[str, object]:
    """Validate one declared settings tree and return it deeply immutable.

    ``value`` is the raw parsed table and ``path`` its dotted config location.
    Keys must be plain identifiers (letters, digits, ``_``, ``-``) so literal
    dotted keys cannot splice namespaces when rendered back. Values may be
    strings, booleans, integers, finite floats, arrays, or nested tables;
    ``None``, dates/times, bytes, non-finite floats, and any other object are
    rejected because they cannot round-trip through both TOML and JSON.
    Returns ``MappingProxyType``-wrapped tables with arrays as tuples;
    malformed input raises ``ValidationError`` naming ``path``.
    """

    return _validate_table(value, path)


def _validate_table(value: object, path: str) -> Mapping[str, object]:
    """Validate one settings table level and return it immutable."""

    if not isinstance(value, Mapping):
        raise ValidationError(f"{path} must be a table of native settings")
    result: dict[str, object] = {}
    for key, item in value.items():
        if not isinstance(key, str) or not _SETTING_KEY.fullmatch(key):
            raise ValidationError(
                f"{path} keys must be plain identifiers without dots: {key!r}"
            )
        result[key] = _validate_value(item, f"{path}.{key}")
    return MappingProxyType(result)


def _validate_value(value: object, path: str) -> object:
    """Validate one scalar/array/table value and return it immutable."""

    if isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValidationError(f"{path} must be a finite float")
        return value
    if isinstance(value, Mapping):
        return _validate_table(value, path)
    if isinstance(value, (list, tuple)):
        return tuple(
            _validate_value(item, f"{path}[{index}]")
            for index, item in enumerate(value)
        )
    raise ValidationError(
        f"{path} must be a string, boolean, integer, finite float, array, "
        "or table; null, dates, and other values cannot round-trip"
    )


def enforce_native_settings(
    settings: object, reserved: frozenset[str], path: str
) -> Mapping[str, object]:
    """Re-validate ``settings`` and reject reserved control roots.

    ``settings`` is re-run through :func:`validate_native_settings` so a
    programmatically built ``RuntimeConfig`` gets the same strict types as a
    parsed one. Every top-level key present in ``reserved`` raises an
    actionable ``ValidationError``; those roots encode model/auth/sandbox/
    hook ownership and are never silently overwritten. Returns the validated
    immutable tree so adapters can merge the checked value.
    """

    validated = validate_native_settings(settings, path)
    runtime = _RESERVED_LABELS.get(id(reserved), "native")
    for key in sorted(validated):
        if key in reserved:
            raise ValidationError(
                f"{path}.{key} is owned by agent-run or the {runtime} "
                "runtime's security model; tune it through agent-run "
                "configuration, not native_settings"
            )
    return validated


def native_settings_json(value: Mapping[str, object]) -> dict[str, object]:
    """Return a deterministic JSON-ready copy of a validated settings tree.

    Tables become sorted plain dicts and arrays become lists so the result is
    stable for snapshot documents and ``json.dumps``; nesting depth follows
    the input. The input must already be validated; the output contains no
    values the validators would have rejected.
    """

    result: dict[str, object] = {}
    for key in sorted(value):
        item = value[key]
        if isinstance(item, Mapping):
            result[key] = native_settings_json(item)
        elif isinstance(item, (list, tuple)):
            result[key] = [
                element
                if not isinstance(element, Mapping)
                else native_settings_json(element)
                for element in item
            ]
        else:
            result[key] = item
    return result
