"""Bounded delivery of trusted terminal workflow notices."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping

from ..config import DeliveryConfig
from ..domain import OrchestratorRef
from ..errors import ValidationError
from ..state.store import StateStore
from .base import AmbiguousDeliveryError, ChatTransport, DeliveryError
from .workflow_notice import WorkflowNotice


@dataclass(frozen=True, slots=True)
class WorkflowDispatchResult:
    """Count workflow notices claimed and completed during one bounded drain."""

    claimed: int = 0
    delivered: int = 0
    retried: int = 0
    failed: int = 0


class WorkflowDeliveryDispatcher:
    """Drain the workflow outbox using the existing transport implementations."""

    def __init__(self, store: StateStore, transports: Mapping[str, ChatTransport],
                 config: DeliveryConfig | None = None, *, owner: str = "workflow-dispatch") -> None:
        """Bind a store, nonempty transport map, retry policy, and lease owner."""

        if not transports:
            raise ValidationError("workflow dispatcher needs transports")
        self._store = store
        self._transports = dict(transports)
        self._config = DeliveryConfig() if config is None else config
        self._owner = owner

    def drain(self, *, at: float | None = None, max_batch: int = 100) -> WorkflowDispatchResult:
        """Claim at most ``max_batch`` due rows and deliver each once per claim."""

        if max_batch < 1:
            raise ValidationError("max_batch must be at least 1")
        claimed = delivered = retried = failed = 0
        while claimed < max_batch:
            row = self._store.claim_workflow_delivery(self._owner, at=at)
            if row is None:
                break
            claimed += 1
            transport = self._transports.get(str(row["transport"]))
            if transport is None:
                self._store.fail_workflow_delivery(str(row["id"]), self._owner,
                                                   "delivery transport is not configured", at=at)
                failed += 1
                continue
            target = OrchestratorRef(str(row["transport"]), str(row["external_session_id"]),
                                     None if row["external_turn_id"] is None else str(row["external_turn_id"]))
            notice = WorkflowNotice(str(row["id"]), str(row["run_id"]), str(row["run_status"]))
            try:
                receipt = transport.send(target, notice)
            except (DeliveryError, AmbiguousDeliveryError) as error:
                self._store.retry_workflow_delivery(str(row["id"]), self._owner, str(error), at=at,
                                                    base_delay=self._config.retry_base_seconds,
                                                    max_delay=self._config.retry_cap_seconds)
                retried += 1
            else:
                self._store.complete_workflow_delivery(str(row["id"]), self._owner,
                                                       remote_message_id=receipt.remote_message_id, at=at)
                delivered += 1
        return WorkflowDispatchResult(claimed, delivered, retried, failed)
