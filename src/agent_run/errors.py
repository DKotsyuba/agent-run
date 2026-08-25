"""Shared agent-run errors."""


class AgentRunError(Exception):
    """Base class for expected agent-run failures."""


class ValidationError(AgentRunError, ValueError):
    """Input violates a public contract."""


class StateTransitionError(ValidationError):
    """An agent state transition is not allowed."""


class PathEscapeError(ValidationError):
    """A derived path escapes its declared root."""
