"""Install declared plugins into the generated Codex home.

Codex refuses to run a plugin hook it has not explicitly trusted, and the
trust prompt has no non-interactive answer. Verified live against
``codex-cli 0.149.1``: a child whose home has the plugin installed and
enabled but no ``[hooks.state]`` entry runs the Bash tool with the hooks
silently inert, and codex never writes the missing trust itself. So
agent-run reproduces codex's own trust digest offline.

The digest is ``sha256`` over the canonical JSON of a normalized identity
``{event_name, matcher?, hooks: [handler]}`` -- keys sorted recursively,
compact separators, absent options omitted rather than null. Reproduced
exactly against four known-good entries in the owner's live
``~/.codex/config.toml``.

Two further shapes are load-bearing and were both established live, not
assumed: the version directory below ``plugins/cache`` must hold real
files (a symlink to the checkout reads as ``not installed``), and the
``personal`` marketplace manifest is resolved from ``$HOME``, which for
this runtime is the generated home itself.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

from ...errors import ValidationError
from ..home import content_hash, write_managed_file

__all__ = ["MARKETPLACE", "install"]

MARKETPLACE = "personal"

_SAFE_TOKEN = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.+-]*\Z")
_HOOKS_REL = "hooks/hooks.json"
_SKIP_DIRS = frozenset({".git", "__pycache__"})
# codex-rs/hooks: a command handler with no explicit timeout is normalized to
# 600 seconds before hashing, and never below one second.
_DEFAULT_TIMEOUT_SEC = 600
_EVENT_LABELS = {
    "PreToolUse": "pre_tool_use",
    "PermissionRequest": "permission_request",
    "PostToolUse": "post_tool_use",
    "PreCompact": "pre_compact",
    "PostCompact": "post_compact",
    "SessionStart": "session_start",
    "UserPromptSubmit": "user_prompt_submit",
    "SubagentStart": "subagent_start",
    "SubagentStop": "subagent_stop",
    "Stop": "stop",
}
# Codex drops the matcher for these events before hashing, so a manifest that
# carries one would otherwise produce a digest the engine never matches.
_MATCHERLESS = frozenset({"user_prompt_submit", "stop"})


def _token(value: object, label: str) -> str:
    if not isinstance(value, str) or not _SAFE_TOKEN.fullmatch(value):
        raise ValidationError(f"codex plugin {label} is not a safe identifier: {value!r}")
    return value


def _manifest(directory: Path) -> tuple[str, str]:
    """Read ``(name, version)`` from the plugin's codex or claude manifest."""

    for relative in (".codex-plugin/plugin.json", ".claude-plugin/plugin.json"):
        try:
            payload = json.loads((directory / relative).read_text(encoding="utf-8"))
        except (OSError, ValueError):
            continue
        if isinstance(payload, dict) and payload.get("name") and payload.get("version"):
            return (
                _token(payload["name"], "name"),
                _token(payload["version"], "version"),
            )
    raise ValidationError(f"codex plugin has no usable manifest: {directory}")


def _handler(raw: object, where: str) -> dict[str, object]:
    """Normalize one hook handler exactly as codex does before hashing."""

    if not isinstance(raw, dict):
        raise ValidationError(f"{where} must be a hook handler table")
    if raw.get("type") != "command":
        raise ValidationError(f"{where} is not a command hook; agent-run cannot trust it")
    command = raw.get("command")
    if not isinstance(command, str) or not command:
        raise ValidationError(f"{where}.command must be a nonblank string")
    if raw.get("additionalContextLimit") is not None:
        raise ValidationError(
            f"{where}.additionalContextLimit is not supported; its trusted digest "
            "depends on a codex-internal default agent-run cannot read"
        )
    timeout = raw.get("timeout", _DEFAULT_TIMEOUT_SEC)
    if isinstance(timeout, bool) or not isinstance(timeout, int):
        raise ValidationError(f"{where}.timeout must be an integer number of seconds")
    asynchronous = raw.get("async", False)
    if not isinstance(asynchronous, bool):
        raise ValidationError(f"{where}.async must be a boolean")
    handler: dict[str, object] = {
        "type": "command",
        "command": command,
        "timeout": max(timeout, 1),
        "async": asynchronous,
    }
    status = raw.get("statusMessage")
    if status is not None:
        if not isinstance(status, str):
            raise ValidationError(f"{where}.statusMessage must be a string")
        handler["statusMessage"] = status
    return handler


def _digest(event: str, matcher: str | None, handler: dict[str, object]) -> str:
    identity: dict[str, object] = {"event_name": event}
    if matcher is not None:
        identity["matcher"] = matcher
    identity["hooks"] = [handler]
    canonical = json.dumps(identity, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return f"sha256:{content_hash(canonical)}"


def _trust_entries(name: str, directory: Path) -> list[tuple[str, str]]:
    """Return ``(state_key, trusted_hash)`` for every hook the plugin declares."""

    source = directory / _HOOKS_REL
    if not source.is_file():
        return []
    try:
        document = json.loads(source.read_text(encoding="utf-8"))
    except (OSError, ValueError) as error:
        raise ValidationError(f"codex plugin {name} has an unreadable {_HOOKS_REL}: {error}") from error
    events = document.get("hooks") if isinstance(document, dict) else None
    if not isinstance(events, dict):
        raise ValidationError(f"codex plugin {name} declares no hooks table in {_HOOKS_REL}")
    entries: list[tuple[str, str]] = []
    for event_name in sorted(events):
        event = _EVENT_LABELS.get(event_name)
        if event is None:
            raise ValidationError(
                f"codex plugin {name} declares an unsupported hook event: {event_name}"
            )
        groups = events[event_name]
        if not isinstance(groups, list):
            raise ValidationError(f"codex plugin {name} hook event {event_name} must be an array")
        for group_index, group in enumerate(groups):
            where = f"codex plugin {name} {event_name}[{group_index}]"
            if not isinstance(group, dict):
                raise ValidationError(f"{where} must be a matcher group table")
            matcher = group.get("matcher")
            if matcher is not None and not isinstance(matcher, str):
                raise ValidationError(f"{where}.matcher must be a string")
            if event in _MATCHERLESS:
                matcher = None
            handlers = group.get("hooks")
            if not isinstance(handlers, list):
                raise ValidationError(f"{where}.hooks must be an array")
            for handler_index, raw in enumerate(handlers):
                handler = _handler(raw, f"{where}.hooks[{handler_index}]")
                key = (
                    f"{name}@{MARKETPLACE}:{_HOOKS_REL}:{event}:{group_index}:{handler_index}"
                )
                entries.append((key, _digest(event, matcher, handler)))
    return entries


def _copy_tree(home: Path, relative: str, directory: Path) -> str:
    """Copy the plugin into the generated home and return a content digest.

    Real files, not a bridge: codex reports a symlinked version directory as
    ``not installed`` and never loads its hooks (verified live).
    """

    digests: list[str] = []
    for source in sorted(directory.rglob("*")):
        parts = source.relative_to(directory).parts
        if any(part in _SKIP_DIRS for part in parts) or source.is_symlink() or not source.is_file():
            continue
        try:
            payload = source.read_bytes()
        except OSError as error:
            raise ValidationError(f"cannot read plugin file {source}: {error}") from error
        digest = write_managed_file(home, f"{relative}/{'/'.join(parts)}", payload)
        digests.append(f"{'/'.join(parts)}:{digest}")
    if not digests:
        raise ValidationError(f"codex plugin directory has no readable files: {directory}")
    return content_hash("\n".join(digests))


def install(home: Path, plugins: tuple[Path, ...]) -> tuple[tuple[str, ...], str]:
    """Materialize declared plugins; return config.toml lines and a fingerprint."""

    if not plugins:
        return (), content_hash("no_plugins")
    lines: list[str] = []
    fingerprint: list[str] = []
    listed: list[dict[str, object]] = []
    for directory in plugins:
        name, version = _manifest(directory)
        relative = f"plugins/cache/{MARKETPLACE}/{name}/{version}"
        tree_digest = _copy_tree(home, relative, directory)
        listed.append(
            {
                "name": name,
                "source": {"source": "local", "path": f"./{relative}"},
                "policy": {"installation": "AVAILABLE", "authentication": "ON_INSTALL"},
            }
        )
        lines.append(f'[plugins."{name}@{MARKETPLACE}"]')
        lines.append("enabled = true")
        lines.append("")
        fingerprint.append(f"{name}@{MARKETPLACE}:{version}:{tree_digest}")
        for key, trusted_hash in _trust_entries(name, directory):
            lines.append(f'[hooks.state."{key}"]')
            lines.append(f'trusted_hash = "{trusted_hash}"')
            lines.append("")
            fingerprint.append(f"{key}={trusted_hash}")
    # ``HOME`` is the generated home for this runtime, and the ``personal``
    # marketplace manifest is resolved from ``$HOME``; without this file the
    # installed copies belong to a marketplace the child cannot see.
    catalog = json.dumps(
        {"name": MARKETPLACE, "interface": {"displayName": "agent-run"}, "plugins": listed},
        sort_keys=True,
    )
    fingerprint.append(
        write_managed_file(home, ".agents/plugins/marketplace.json", catalog)
    )
    return tuple(lines), content_hash("\n".join(fingerprint))
