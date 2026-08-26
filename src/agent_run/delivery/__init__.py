"""Completion delivery: trusted notice, transports, and the outbox dispatcher."""

from .base import (
    NOTICE_VERSION,
    TRANSPORT_API_VERSION,
    AmbiguousDeliveryError,
    ChatTransport,
    ChatTransportConfig,
    CompletionNotice,
    DeliveryError,
    DeliveryReceipt,
)
from .claude_uds import ClaudeSessionSender, ClaudeUdsTransport
from .codex_queue import TRANSPORT_NAME, CodexQueueTransport
from .dispatch import (
    DeliveryDispatcher,
    DispatchResult,
    dispatcher_lock,
    dispatcher_lock_path,
)

__all__ = [
    "NOTICE_VERSION",
    "TRANSPORT_API_VERSION",
    "TRANSPORT_NAME",
    "AmbiguousDeliveryError",
    "ChatTransport",
    "ChatTransportConfig",
    "ClaudeSessionSender",
    "ClaudeUdsTransport",
    "CodexQueueTransport",
    "CompletionNotice",
    "DeliveryDispatcher",
    "DeliveryError",
    "DeliveryReceipt",
    "DispatchResult",
    "dispatcher_lock",
    "dispatcher_lock_path",
]
