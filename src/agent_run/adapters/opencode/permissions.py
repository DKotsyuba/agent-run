"""Permission decisions for the managed OpenCode service."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Mapping

from ...errors import ValidationError
from .normalize import _sequence


EXTERNAL_DIRECTORY = "external_directory"


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
        if not isinstance(permission, Mapping):
            raise ValidationError("permission must be a mapping")
        identifier = permission_id(permission)
        kind = permission.get("type")
        if kind != EXTERNAL_DIRECTORY:
            return self._block(identifier, str(kind), f"permission type is auto-rejected: {kind!r}")
        if self._granted is not None:
            return self._block(
                identifier, kind, "external_directory was already granted once for this agent"
            )
        resolved = _resolve_directory(permission.get("path"))
        if resolved is None:
            return self._block(identifier, kind, "external_directory path is not usable")
        if not any(_contains(root, resolved) for root in self.read_roots):
            return self._block(
                identifier, kind, f"external_directory {resolved} is outside the resolved read roots"
            )
        self._granted = str(resolved)
        return PermissionDecision(identifier, True, f"external_directory contained by read roots: {resolved}")

    def _block(self, identifier: str, kind: str, reason: str) -> PermissionDecision:
        self._blocked[kind] = self._blocked.get(kind, 0) + 1
        return PermissionDecision(identifier, False, reason)

    def blocked_summary(self) -> Mapping[str, int]:
        """Bounded counts of rejected permissions, by requested type."""

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
    items = payload.get("permissions") if isinstance(payload, Mapping) else payload
    result = []
    for item in _sequence(items):
        if not isinstance(item, Mapping):
            raise ValidationError("permission must be a mapping")
        result.append(item)
    return tuple(result)


def _contains(root: Path, candidate: Path) -> bool:
    return candidate == root or candidate.is_relative_to(root)


def _resolve_directory(value: object) -> Path | None:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        path = Path(value).expanduser()
        if not path.is_absolute():
            return None
        return path.resolve()
    except (OSError, RuntimeError):
        return None
