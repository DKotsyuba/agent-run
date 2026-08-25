"""On-demand completion delivery dispatcher owned by a single flock holder."""

from __future__ import annotations

import fcntl
import os
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator, Mapping

from ..config import DeliveryConfig
from ..domain import AgentStatus, OrchestratorRef
from ..errors import ValidationError
from ..paths import agent_run_home
from ..state.db import finish_delivery_claim, immediate, timestamp
from ..state.store import StateStore
from .base import (
    AmbiguousDeliveryError,
    ChatTransport,
    CompletionNotice,
    DeliveryError,
)


LOCK_NAME = "delivery-dispatcher.lock"
DEFAULT_LEASE_SECONDS = 30.0
DEFAULT_MAX_BATCH = 1000


def dispatcher_lock_path(home: str | Path | None = None) -> Path:
    return agent_run_home(home) / "locks" / LOCK_NAME


@contextmanager
def dispatcher_lock(path: Path) -> Iterator[bool]:
    """Yield True to the single owner; yield False when another run holds it."""

    path.parent.mkdir(parents=True, exist_ok=True)
    handle = open(path, "a+")
    try:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            yield False
            return
        try:
            yield True
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
    finally:
        handle.close()


def new_owner() -> str:
    return f"disp-{os.getpid()}-{uuid.uuid4().hex[:8]}"


@dataclass(frozen=True, slots=True)
class DispatchResult:
    claimed: int = 0
    delivered: int = 0
    retried: int = 0
    failed: int = 0
    ambiguous: int = 0
    locked_out: bool = False


def notice_for(row: Mapping[str, object]) -> CompletionNotice:
    return CompletionNotice(
        notification_id=str(row["id"]),
        agent_id=str(row["agent_id"]),
        status=AgentStatus(str(row["agent_status"])),
    )


def target_for(row: Mapping[str, object]) -> OrchestratorRef:
    turn_id = row.get("external_turn_id")
    return OrchestratorRef(
        transport=str(row["transport"]),
        external_session_id=str(row["external_session_id"]),
        external_turn_id=None if turn_id is None else str(turn_id),
    )


class DeliveryDispatcher:
    """Drain the delivery outbox once, then exit.

    The dispatcher is on-demand: it is started when a notification becomes
    pending and it returns as soon as nothing is claimable. Exactly one run
    works at a time, enforced by a non-blocking flock; a second run reports
    `locked_out` instead of competing for leases.
    """

    def __init__(
        self,
        store: StateStore,
        transports: Mapping[str, ChatTransport],
        config: DeliveryConfig | None = None,
        *,
        lease_seconds: float = DEFAULT_LEASE_SECONDS,
        owner: str | None = None,
    ) -> None:
        if not isinstance(store, StateStore):
            raise ValidationError("store must be a StateStore")
        if not isinstance(transports, Mapping) or not transports:
            raise ValidationError("transports must be a nonempty mapping")
        self._store = store
        self._transports = dict(transports)
        self._config = DeliveryConfig() if config is None else config
        if not isinstance(self._config, DeliveryConfig):
            raise ValidationError("config must be a DeliveryConfig")
        self._lease_seconds = lease_seconds
        self._owner = new_owner() if owner is None else owner

    @property
    def owner(self) -> str:
        return self._owner

    def run(
        self,
        *,
        lock_path: Path | None = None,
        home: str | Path | None = None,
        at: float | None = None,
        max_batch: int = DEFAULT_MAX_BATCH,
    ) -> DispatchResult:
        path = dispatcher_lock_path(home) if lock_path is None else lock_path
        with dispatcher_lock(path) as owned:
            if not owned:
                return DispatchResult(locked_out=True)
            return self.drain(at=at, max_batch=max_batch)

    def drain(
        self, *, at: float | None = None, max_batch: int = DEFAULT_MAX_BATCH
    ) -> DispatchResult:
        """Claim and dispatch until nothing is due. Assumes the lock is held."""

        if max_batch < 1:
            raise ValidationError("max_batch must be at least 1")
        claimed = delivered = retried = failed = ambiguous = 0
        while claimed < max_batch:
            row = self._store.claim_delivery(
                self._owner, at=at, lease_seconds=self._lease_seconds
            )
            if row is None:
                break
            claimed += 1
            verdict = self._dispatch(row, at)
            if verdict.ambiguous:
                ambiguous += 1
            if verdict.state == "delivered":
                delivered += 1
            elif verdict.state == "retry_wait":
                retried += 1
            else:
                failed += 1
        return DispatchResult(
            claimed=claimed,
            delivered=delivered,
            retried=retried,
            failed=failed,
            ambiguous=ambiguous,
        )

    def _dispatch(self, row: Mapping[str, object], at: float | None) -> _Verdict:
        delivery_id = str(row["id"])
        transport = self._transports.get(str(row["transport"]))
        if transport is None:
            return self._give_up_or_retry(
                row, f"no transport named {row['transport']!r}", False, at
            )
        try:
            receipt = transport.send(target_for(row), notice_for(row))
        except AmbiguousDeliveryError as error:
            # At-least-once: the message may have arrived, so record the
            # ambiguity and try again. A duplicate wake names the same agent.
            return self._give_up_or_retry(row, str(error), True, at)
        except DeliveryError as error:
            return self._give_up_or_retry(row, str(error), False, at)
        self._store.complete_delivery(
            delivery_id,
            self._owner,
            remote_message_id=receipt.remote_message_id,
            ambiguous_result=receipt.ambiguous,
            at=at,
        )
        return _Verdict("delivered", receipt.ambiguous)

    def _give_up_or_retry(
        self,
        row: Mapping[str, object],
        error: str,
        ambiguous: bool,
        at: float | None,
    ) -> _Verdict:
        delivery_id = str(row["id"])
        limit = self._config.max_attempts
        # `max_attempts = 0` means unlimited: retries are durable by default.
        if limit > 0 and int(row["attempts"]) >= limit:
            self._fail(delivery_id, error, ambiguous, at)
            return _Verdict("failed", ambiguous)
        self._store.retry_delivery(
            delivery_id,
            self._owner,
            error,
            at=at,
            ambiguous_result=ambiguous,
            base_delay=self._config.retry_base_seconds,
            max_delay=self._config.retry_cap_seconds,
        )
        return _Verdict("retry_wait", ambiguous)

    def _fail(
        self, delivery_id: str, error: str, ambiguous: bool, at: float | None
    ) -> None:
        # StateStore exposes delivered/retry_wait/cancelled only; `failed` is
        # reached through the same guarded claim helper it uses itself.
        connection = self._store.connection
        now = timestamp(at)
        with immediate(connection):
            if not finish_delivery_claim(
                connection,
                delivery_id,
                self._owner,
                "failed",
                now=now,
                last_error=error,
                ambiguous_result=ambiguous,
            ):
                raise ValidationError("delivery lease is not owned by caller")


@dataclass(frozen=True, slots=True)
class _Verdict:
    state: str
    ambiguous: bool
