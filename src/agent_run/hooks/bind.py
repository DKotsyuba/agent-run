"""Synchronous hook that binds a started agent to its orchestration session."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping

from ..domain import AgentId, OrchestratorRef, validate_agent_id
from ..errors import AgentRunError, ValidationError
from ..state.store import StateStore


_PAYLOAD_KEYS = frozenset(
    {"agent_id", "transport", "external_session_id", "external_turn_id"}
)


class BindHookError(AgentRunError):
    """Binding failed. The hook says so out loud instead of returning quietly."""


def unconfirmed(agent_id: object, reason: str) -> str:
    """The explicit report a failing hook owes the orchestrator."""

    return (
        f"agent-run: chat notification is NOT confirmed for {agent_id}: {reason}. "
        f"The agent may still be running; keep this turn alive and recover with "
        f"agent-run bind."
    )


@dataclass(frozen=True, slots=True)
class BindResult:
    agent_id: AgentId
    session_id: str

    def message(self) -> str:
        return (
            f"agent-run: agent {self.agent_id} is bound to session "
            f"{self.session_id}; its completion will be delivered to this chat."
        )


def ref_from_payload(payload: Mapping[str, object]) -> OrchestratorRef:
    if not isinstance(payload, Mapping):
        raise ValidationError("bind payload must be a mapping")
    unknown = set(payload) - _PAYLOAD_KEYS
    if unknown:
        raise ValidationError(f"unknown bind payload keys: {sorted(unknown)}")
    turn_id = payload.get("external_turn_id")
    return OrchestratorRef(
        transport=payload.get("transport"),  # type: ignore[arg-type]
        external_session_id=payload.get("external_session_id"),  # type: ignore[arg-type]
        external_turn_id=None if turn_id is None else turn_id,  # type: ignore[arg-type]
    )


def bind(
    store: StateStore,
    agent_id: str | AgentId,
    ref: OrchestratorRef,
    *,
    at: float | None = None,
) -> BindResult:
    """Bind once. Empty and identical targets succeed; a different one refuses.

    A terminal agent that finished before binding has a delivery waiting; the
    store activates it inside the same transaction, exactly once.
    """

    if not isinstance(store, StateStore):
        raise ValidationError("store must be a StateStore")
    if not isinstance(ref, OrchestratorRef):
        raise ValidationError("ref must be an OrchestratorRef")
    checked = validate_agent_id(agent_id)
    try:
        session_id = store.bind_orchestrator(checked, ref, at=at)
    except (ValidationError, LookupError) as error:
        raise BindHookError(unconfirmed(checked, str(error))) from error
    return BindResult(agent_id=checked, session_id=session_id)


def run_hook(
    store: StateStore,
    payload: Mapping[str, object],
    *,
    at: float | None = None,
) -> BindResult:
    """Entry point for the runtime PostToolUse hook. Raises loudly on failure."""

    try:
        agent_id = payload["agent_id"] if isinstance(payload, Mapping) else None
        ref = ref_from_payload(payload)
    except (ValidationError, KeyError, TypeError) as error:
        raise BindHookError(
            unconfirmed(
                payload.get("agent_id") if isinstance(payload, Mapping) else None,
                f"unusable hook payload ({error})",
            )
        ) from error
    try:
        checked = validate_agent_id(agent_id)  # type: ignore[arg-type]
    except ValidationError as error:
        raise BindHookError(unconfirmed(agent_id, str(error))) from error
    return bind(store, checked, ref, at=at)
