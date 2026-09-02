"""Shared agent-run errors."""


class AgentRunError(Exception):
    """Base class for expected agent-run failures."""


class BrokerUnavailable(AgentRunError):
    """The resident agent-run broker cannot be reached."""


class ValidationError(AgentRunError, ValueError):
    """Input violates a public contract."""


class StateTransitionError(ValidationError):
    """An agent state transition is not allowed."""


class PathEscapeError(ValidationError):
    """A derived path escapes its declared root."""


class SchemaMigrationRequired(ValidationError):
    """The state store predates this code and has not been migrated yet.

    Raised only by read-only callers, which must not write.  Any path that
    opens the store for writing migrates it instead of raising.
    """

    def __init__(self, found: int, expected: int):
        super().__init__(
            f"state schema is v{found} and this agent-run needs v{expected}; "
            "the next command that opens the store for writing migrates it"
        )
        self.found = found
        self.expected = expected


class AuthError(AgentRunError):
    """A runtime could not obtain a usable credential."""


class CapacitySourceError(AgentRunError):
    """A capacity evidence source failed its own collection contract.

    ``reason`` is a fixed, provider-independent reason code (for example
    ``codexbar_nonzero_exit``) safe for logs and reports: it never carries
    raw provider output, tokens, or exception text.
    """

    def __init__(self, reason: str):
        super().__init__(reason)
        self.reason = reason
