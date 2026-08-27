"""Permission decisions for the managed OpenCode service."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Mapping

from ...errors import ValidationError
from .normalize import _sequence


EXTERNAL_DIRECTORY = "external_directory"
#: Blocked-summary key for a request whose ``action`` is missing or malformed.
#: Never the string ``"None"``: that was the old v2 parser reading a key v1
#: does not send, and it made every durable ``permissions_blocked`` event
#: useless (proven live: ``permissions_blocked {"None": 2}``).
UNKNOWN_ACTION = "unknown"


@dataclass(frozen=True)
class PermissionDecision:
    permission_id: str
    granted: bool
    reason: str


@dataclass
class PermissionBroker:
    """Auto-reject every interactive permission except one contained grant."""

    read_roots: tuple[Path, ...] = ()
    _granted: str | None = field(default=None, init=False)
    _blocked: dict[str, int] = field(default_factory=dict, init=False)

    def __post_init__(self) -> None:
        for index, root in enumerate(self.read_roots):
            if not isinstance(root, Path) or not root.is_absolute():
                raise ValidationError(f"read_roots[{index}] must be an absolute path")
        self.read_roots = tuple(self.read_roots)

    @property
    def granted_directory(self) -> str | None:
        return self._granted

    def decide(self, permission: Mapping[str, object]) -> PermissionDecision:
        """Decide one v1 ``PermissionV2Request``.

        The live v1 1.18.18 shape is ``{id, sessionID, action, resources,
        save?, metadata?, source?}``: no ``type``, no ``path`` (proven live
        against the canary service). Reading the v2 keys is what auto-rejected
        every permission and recorded it under the literal key ``"None"``.
        ``resources`` is a list of glob-ish strings -- an ``external_directory``
        ask carries exactly ``["/abs/dir/*"]``.

        A sub-agent is not filtered here because it cannot be: a
        ``PermissionV2Request`` carries no agent field. The generated config
        denies ``external_directory`` outright for the verify sub-agent, so a
        sub-agent's ask never reaches this broker at all.
        """

        if not isinstance(permission, Mapping):
            raise ValidationError("permission must be a mapping")
        identifier = permission_id(permission)
        raw_action = permission.get("action")
        action = raw_action if isinstance(raw_action, str) and raw_action.strip() else UNKNOWN_ACTION
        if action != EXTERNAL_DIRECTORY:
            return self._block(
                identifier, action, f"permission action is auto-rejected: {raw_action!r}"
            )
        if self._granted is not None:
            return self._block(
                identifier, action, "external_directory was already granted once for this agent"
            )
        resources = _resolve_resources(permission.get("resources"))
        if not resources:
            return self._block(identifier, action, "external_directory names no usable directory")
        for resource in resources:
            if not any(_contains(root, resource) for root in self.read_roots):
                return self._block(
                    identifier,
                    action,
                    f"external_directory {resource} is outside the resolved read roots",
                )
        self._granted = str(resources[0])
        return PermissionDecision(
            identifier, True, f"external_directory contained by read roots: {self._granted}"
        )

    def _block(self, identifier: str, kind: str, reason: str) -> PermissionDecision:
        self._blocked[kind] = self._blocked.get(kind, 0) + 1
        return PermissionDecision(identifier, False, reason)

    def blocked_summary(self) -> Mapping[str, int]:
        """Bounded counts of rejected permissions, keyed by the v1 ``action``."""

        # A plain dict, not MappingProxyType: this value is handed straight
        # to EventSink.event() for the durable "permissions_blocked" event,
        # and only a plain dict/list/str/int/bool/None tree is JSON-safe.
        return dict(sorted(self._blocked.items()))

    def reply(self, decision: PermissionDecision) -> Mapping[str, object]:
        """The exact body the v2 permission reply endpoint accepts."""

        # A plain dict, not MappingProxyType: this value is passed straight
        # to OpenCodeHttpClient.answer_permission(), whose request encoding
        # calls json.dumps() directly on it with no dict()-unwrapping step --
        # unlike a MappingProxyType, only a plain dict is JSON serializable.
        return {"reply": "once" if decision.granted else "reject"}


def permission_id(permission: Mapping[str, object]) -> str:
    identifier = permission.get("id")
    if not isinstance(identifier, str) or not identifier.strip():
        raise ValidationError("permission id must be a nonblank string")
    return identifier


def permission_items(payload: object) -> tuple[Mapping[str, object], ...]:
    if payload is None:
        return ()
    # v1 replies ``{"data": [PermissionV2Request, ...]}``; a bare array is
    # accepted too, so the caller may hand over either the whole reply or the
    # array it already unwrapped.
    items = payload.get("data") if isinstance(payload, Mapping) else payload
    result = []
    for item in _sequence(items):
        if not isinstance(item, Mapping):
            raise ValidationError("permission must be a mapping")
        result.append(item)
    return tuple(result)


def _contains(root: Path, candidate: Path) -> bool:
    return candidate == root or candidate.is_relative_to(root)


def _resolve_resources(value: object) -> tuple[Path, ...]:
    """Every requested resource as an absolute directory, or ``()``.

    v1 states an ``external_directory`` resource as the directory's glob --
    ``"/abs/dir/*"`` (proven live). A resource that is not an absolute path
    once that trailing glob is dropped makes the whole request unusable, so
    the caller rejects it rather than granting a fraction of it.
    """

    resolved: list[Path] = []
    for item in _sequence(value):
        if not isinstance(item, str) or not item.strip():
            return ()
        text = item.strip()
        while text.endswith("*"):
            text = text[:-1].rstrip("/") or "/"
        try:
            path = Path(text).expanduser()
            if not path.is_absolute():
                return ()
            resolved.append(path.resolve())
        except (OSError, RuntimeError):
            return ()
    return tuple(resolved)
