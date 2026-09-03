"""On-demand completion delivery dispatcher owned by a single flock holder."""

from __future__ import annotations

import fcntl
import json
import logging
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
from ..state.store import StateStore
from .base import (
    MAX_METADATA_LENGTH,
    AmbiguousDeliveryError,
    ChatTransport,
    CompletionNotice,
    DeliveryAttemptEvidence,
    DeliveryError,
)

_logger = logging.getLogger("agent_run.delivery")


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
        except BlockingIOError:
            yield False
            return
        except OSError as error:
            raise DeliveryError("cannot acquire delivery dispatcher lock") from error
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
    claim_lost: int = 0
    locked_out: bool = False


def _row_value(row: Mapping[str, object], key: str) -> object:
    """Return one row column, or ``None`` when an older stored row lacks it.

    Accepts both :class:`sqlite3.Row` (raises ``IndexError``) and mapping
    rows from tests (raise ``KeyError``) so notice construction never breaks
    on a row projected before launch metadata existed.
    """

    try:
        return row[key]
    except (KeyError, IndexError):
        return None


def _bounded_metadata(value: object) -> str | None:
    """Degrade untrustworthy launch metadata to ``None`` instead of failing a send."""

    if isinstance(value, str) and value.strip() and len(value) <= MAX_METADATA_LENGTH:
        return value
    return None


def _effort_from_request_json(raw: object) -> str | None:
    """Extract one bounded effort from stored request JSON, tolerating old rows.

    Missing, malformed, or non-object request JSON, and a non-string or
    oversized ``effort`` value, all degrade to ``None`` (rendered as
    ``unspecified``) so a queued notice is still delivered. Parsing uses the
    standard library alone; no SQLite JSON1 dependency is introduced.
    """

    if not isinstance(raw, str):
        return None
    try:
        parsed = json.loads(raw)
    except ValueError:
        return None
    if not isinstance(parsed, dict):
        return None
    return _bounded_metadata(parsed.get("effort"))


def notice_for(row: Mapping[str, object]) -> CompletionNotice:
    """Build a notice from a claimed mapping or sqlite row without side effects.

    Required lifecycle columns are id, agent_id, and agent_status; invalid
    facts raise the existing validation errors. Optional launch columns are
    bounded, and only effort is extracted from request JSON. Missing or
    malformed optional data becomes unknown/unspecified rather than blocking
    an already queued delivery.
    """
    return CompletionNotice(
        notification_id=str(row["id"]),
        agent_id=str(row["agent_id"]),
        status=AgentStatus(str(row["agent_status"])),
        runtime=_bounded_metadata(_row_value(row, "agent_runtime")),
        model=_bounded_metadata(_row_value(row, "agent_model")),
        effort=_effort_from_request_json(_row_value(row, "agent_request_json")),
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
        for name in ("retry_base_seconds", "retry_cap_seconds"):
            value = getattr(self._config, name)
            if type(value) not in (int, float) or not 0 < value < float("inf"):
                raise ValidationError(f"delivery.{name} must be positive and finite")
        if self._config.retry_cap_seconds < self._config.retry_base_seconds:
            raise ValidationError("delivery.retry_cap_seconds must not be below retry base")
        if type(self._config.max_attempts) is not int or self._config.max_attempts < 0:
            raise ValidationError("delivery.max_attempts must be a nonnegative integer")
        if type(lease_seconds) not in (int, float) or not 0 < lease_seconds < float("inf"):
            raise ValidationError("lease_seconds must be positive and finite")
        for transport in self._transports.values():
            transport.validate(self._config)
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
        # Retire the notices that can never be dispatched before claiming
        # anything, so a backlog of them cannot crowd out live work.
        for delivery_id in self._store.expire_unbound_deliveries(at=at):
            _logger.info(
                "dispatch expired delivery_id=%s reason=binding_window_elapsed",
                delivery_id,
            )
        claimed = delivered = retried = failed = ambiguous = claim_lost = 0
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
            elif verdict.state == "claim_lost":
                claim_lost += 1
                break
            else:
                failed += 1
        if claimed:
            _logger.info(
                "drain claimed=%d delivered=%d retried=%d failed=%d ambiguous=%d claim_lost=%d",
                claimed, delivered, retried, failed, ambiguous, claim_lost,
            )
        return DispatchResult(
            claimed=claimed,
            delivered=delivered,
            retried=retried,
            failed=failed,
            ambiguous=ambiguous,
            claim_lost=claim_lost,
        )

    def _dispatch(self, row: Mapping[str, object], at: float | None) -> _Verdict:
        delivery_id = str(row["id"])
        _logger.debug(
            "dispatch attempt delivery_id=%s agent_id=%s transport=%s",
            delivery_id, row.get("agent_id"), row["transport"],
        )
        transport = self._transports.get(str(row["transport"]))
        if transport is None:
            if self._fail(
                delivery_id, "delivery transport is not configured", False, at, None
            ):
                return _Verdict("failed", False)
            return _Verdict("claim_lost", True)
        try:
            receipt = transport.send(target_for(row), notice_for(row))
        except AmbiguousDeliveryError as error:
            # At-least-once: the message may have arrived, so record the
            # ambiguity and try again. A duplicate wake names the same agent.
            return self._give_up_or_retry(row, str(error), True, at, error.evidence)
        except DeliveryError as error:
            return self._give_up_or_retry(row, str(error), False, at, error.evidence)
        except Exception as error:
            return self._give_up_or_retry(
                row, f"transport send raised {type(error).__name__}", True, at, None
            )
        try:
            self._store.complete_delivery(
                delivery_id,
                self._owner,
                remote_message_id=receipt.remote_message_id,
                ambiguous_result=receipt.ambiguous,
                evidence=receipt.evidence,
                at=at,
            )
        except ValidationError:
            return _Verdict("claim_lost", True)
        _logger.info(
            "dispatch delivered delivery_id=%s ambiguous=%s", delivery_id, receipt.ambiguous
        )
        return _Verdict("delivered", receipt.ambiguous)

    def _give_up_or_retry(
        self,
        row: Mapping[str, object],
        error: str,
        ambiguous: bool,
        at: float | None,
        evidence: DeliveryAttemptEvidence | None,
    ) -> _Verdict:
        delivery_id = str(row["id"])
        limit = self._config.max_attempts
        # `max_attempts = 0` means unlimited: retries are durable by default.
        if limit > 0 and int(row["attempts"]) >= limit:
            _logger.warning(
                "dispatch give_up delivery_id=%s attempts=%s reason=%s",
                delivery_id, row["attempts"], error,
            )
            if self._fail(delivery_id, error, ambiguous, at, evidence):
                return _Verdict("failed", ambiguous)
            return _Verdict("claim_lost", True)
        try:
            self._store.retry_delivery(
                delivery_id,
                self._owner,
                error,
                at=at,
                ambiguous_result=ambiguous,
                evidence=evidence,
                base_delay=self._config.retry_base_seconds,
                max_delay=self._config.retry_cap_seconds,
            )
        except ValidationError:
            return _Verdict("claim_lost", True)
        _logger.warning("dispatch retry delivery_id=%s reason=%s", delivery_id, error)
        return _Verdict("retry_wait", ambiguous)

    def _fail(
        self,
        delivery_id: str,
        error: str,
        ambiguous: bool,
        at: float | None,
        evidence: DeliveryAttemptEvidence | None,
    ) -> bool:
        try:
            self._store.fail_delivery(
                delivery_id,
                self._owner,
                error,
                at=at,
                ambiguous_result=ambiguous,
                evidence=evidence,
            )
        except ValidationError:
            return False
        return True


@dataclass(frozen=True, slots=True)
class _Verdict:
    state: str
    ambiguous: bool
