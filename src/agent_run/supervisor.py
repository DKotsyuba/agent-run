"""The detached per-agent supervisor loop."""

from __future__ import annotations

import json
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping

from .adapters.base import Capability, LaunchPlan, RuntimeAdapter, RuntimeSession
from .domain import AgentId, AgentStatus, Message, Outcome, validate_agent_id
from .errors import AgentRunError
from .lifecycle import (
    Deadline,
    Phase,
    ProcessOps,
    ReadyChannel,
    SystemProcessOps,
    checked_pgid,
    install_signal_handlers,
    restore_signal_handlers,
    terminate_process_group,
)
from .state.store import StateStore
from .verify import (
    DEFAULT_SENTINEL,
    STOP_CANCEL,
    STOP_TIMEOUT,
    inspect_answer,
    verify_completion,
)


CANCEL_COMMAND = "cancel"
STEER_COMMAND = "steer"
_MINIMUM_POLL = 0.001
DEFAULT_WARNING_TEXT = (
    "Time budget is nearly exhausted. Stop exploring, write your final answer now."
)


def supervisor_identity() -> str:
    """The full command identity reconciliation compares against."""

    return " ".join(sys.argv) or "agent-run-supervisor"


@dataclass(frozen=True)
class SupervisorSettings:
    heartbeat_seconds: float = 5.0
    poll_seconds: float = 0.25
    grace_seconds: float = 10.0
    kill_grace_seconds: float = 5.0
    warning_fraction: float = 0.90
    warning_text: str = DEFAULT_WARNING_TEXT
    silence_threshold_seconds: float = 60.0
    sentinel: str | None = DEFAULT_SENTINEL


class StoreEventSink:
    """Adapter-facing sink that persists messages and tracks last progress."""

    def __init__(self, store: StateStore, agent_id: AgentId, ops: ProcessOps):
        self._store = store
        self._agent_id = agent_id
        self._ops = ops
        self.last_progress_at: float | None = None
        self.runtime_session_id: str | None = None

    def _touch(self) -> None:
        self.last_progress_at = self._ops.monotonic()

    def message(self, message: Message) -> None:
        self._store.append_message(self._agent_id, message)
        self._touch()

    def session(self, runtime_session_id: str) -> None:
        self.runtime_session_id = runtime_session_id
        self._store.append_event(
            self._agent_id, "runtime_session", data={"id": runtime_session_id}
        )
        self._touch()

    def event(self, kind: str, data: Mapping[str, object]) -> None:
        self._store.append_event(self._agent_id, kind, data=dict(data))
        self._touch()


class Supervisor:
    """Owns one engine process group from launch to durable terminal state."""

    def __init__(
        self,
        store: StateStore,
        agent_id: str | AgentId,
        adapter: RuntimeAdapter,
        plan: LaunchPlan,
        *,
        answer_path: str | Path,
        timeout_seconds: float,
        settings: SupervisorSettings | None = None,
        ops: ProcessOps | None = None,
        ready: ReadyChannel | None = None,
        identity: str | None = None,
        supervisor_pid: int | None = None,
    ):
        self._store = store
        self._agent_id = validate_agent_id(agent_id)
        self._adapter = adapter
        self._plan = plan
        self._answer_path = Path(answer_path)
        self._timeout_seconds = timeout_seconds
        self._settings = settings or SupervisorSettings()
        self._ops = ops or SystemProcessOps()
        self._ready = ready
        self._identity = identity or supervisor_identity()
        self._pid = os.getpid() if supervisor_pid is None else supervisor_pid
        self._sink = StoreEventSink(store, self._agent_id, self._ops)
        self._pgid: int | None = None
        self._stop_reason: str | None = None
        self._signalled = False
        self._warned = False
        self._last_heartbeat: float | None = None

    def run(self) -> Outcome:
        """Report ready only after handlers are armed and `starting` is durable."""

        previous = install_signal_handlers(self._on_signal)
        try:
            self._store.transition(
                self._agent_id, AgentStatus.STARTING, kind="supervisor_starting"
            )
            if self._ready is not None:
                self._ready.ready()
            return self._launch_and_supervise()
        finally:
            restore_signal_handlers(previous)

    def _on_signal(self, received: int) -> None:
        self._signalled = True

    def _launch_and_supervise(self) -> Outcome:
        steerable = Capability.STEER in self._adapter.describe().capabilities
        try:
            session = self._adapter.launch(self._plan, self._sink)
        except Exception as error:  # adapter faults must stay durable, not crash
            return self._commit(
                Outcome(
                    AgentStatus.FAILED,
                    failure_kind="launch_failed",
                    failure_text=str(error),
                )
            )
        if session.pid is None:
            self._safe_cancel(session)
            return self._commit(
                Outcome(AgentStatus.FAILED, failure_kind="no_process_group")
            )
        self._pgid = checked_pgid(session.pid)
        started_at = self._ops.monotonic()
        self._store.record_supervisor(
            self._agent_id,
            pid=self._pid,
            identity=self._identity,
            process_group_id=self._pgid,
        )
        self._last_heartbeat = started_at
        self._store.transition(self._agent_id, AgentStatus.RUNNING, kind="running")
        deadline = Deadline(
            started_at, self._timeout_seconds, self._settings.warning_fraction
        )
        session_outcome = self._supervise(session, deadline, steerable)
        return self._finish(session, session_outcome)

    def _supervise(
        self, session: RuntimeSession, deadline: Deadline, steerable: bool
    ) -> Outcome | None:
        while True:
            self._drain_commands(session, steerable)
            now = self._ops.monotonic()
            if self._stop_reason is None:
                if self._signalled:
                    self._stop_reason = STOP_CANCEL
                else:
                    phase = deadline.phase(now)
                    if phase is Phase.WARNING and not self._warned:
                        self._warn(session, steerable, deadline, now)
                    elif phase is Phase.EXPIRED:
                        self._stop_reason = STOP_TIMEOUT
            if self._stop_reason is not None:
                return self._stop(session)
            outcome = session.wait(self._wait_timeout(deadline, now))
            if outcome is not None:
                return outcome
            self._heartbeat(self._ops.monotonic())

    def _wait_timeout(self, deadline: Deadline, now: float) -> float:
        """Never sleep past the warning point, so a coarse poll cannot skip it."""

        boundary = deadline.expires_at
        if not self._warned and now < deadline.warning_at:
            boundary = deadline.warning_at
        return max(_MINIMUM_POLL, min(self._settings.poll_seconds, boundary - now))

    def _heartbeat(self, now: float) -> None:
        if (
            self._last_heartbeat is not None
            and now - self._last_heartbeat < self._settings.heartbeat_seconds
        ):
            return
        self._last_heartbeat = now
        assert self._pgid is not None
        self._store.record_supervisor(
            self._agent_id,
            pid=self._pid,
            identity=self._identity,
            process_group_id=self._pgid,
        )

    def _warn(
        self, session: RuntimeSession, steerable: bool, deadline: Deadline, now: float
    ) -> None:
        """Issue the single model-visible completion nudge at the warning point."""

        self._warned = True
        delivered = False
        detail: str | None = None
        if steerable:
            try:
                session.steer(self._settings.warning_text)
                delivered = True
            except Exception as error:  # a refused steer is evidence, not a crash
                detail = str(error)
        self._store.append_event(
            self._agent_id,
            "deadline_warning",
            data={
                "delivered": delivered,
                "remaining_seconds": round(deadline.remaining(now), 3),
                "error": detail,
            },
        )

    def _drain_commands(self, session: RuntimeSession, steerable: bool) -> None:
        while True:
            command = self._store.claim_command(self._agent_id)
            if command is None:
                return
            kind = str(command["kind"])
            payload = self._payload(command)
            if kind == CANCEL_COMMAND:
                self._stop_reason = STOP_CANCEL
                result: dict[str, object] = {"accepted": True}
            elif kind == STEER_COMMAND:
                result = self._apply_steer(session, steerable, payload)
            else:
                result = {"accepted": False, "reason": "unsupported_command"}
            self._store.complete_command(int(command["id"]), self._agent_id, result)

    @staticmethod
    def _payload(command: Mapping[str, object]) -> dict[str, object]:
        raw = command.get("payload_json")
        if not isinstance(raw, str) or not raw.strip():
            return {}
        try:
            decoded = json.loads(raw)
        except json.JSONDecodeError:
            return {}
        return decoded if isinstance(decoded, dict) else {}

    def _apply_steer(
        self,
        session: RuntimeSession,
        steerable: bool,
        payload: Mapping[str, object],
    ) -> dict[str, object]:
        text = payload.get("text")
        if not isinstance(text, str) or not text.strip():
            return {"accepted": False, "reason": "empty_steer_text"}
        if not steerable:
            return {"accepted": False, "reason": "capability_unavailable"}
        try:
            session.steer(text)
        except Exception as error:  # keep the command durable and answered
            return {"accepted": False, "reason": "steer_failed", "error": str(error)}
        return {"accepted": True}

    def _stop(self, session: RuntimeSession) -> Outcome | None:
        """Adapter-native control first; group signals are the enforcement."""

        reason = self._stop_reason
        if reason == STOP_CANCEL:
            status = AgentStatus(self._store.get_agent(self._agent_id)["status"])
            if status is AgentStatus.RUNNING:
                self._store.transition(
                    self._agent_id, AgentStatus.CANCELLING, kind="cancelling"
                )
        self._store.append_event(self._agent_id, "stopping", data={"reason": reason})
        self._safe_cancel(session)
        return self._await_exit(session)

    def _safe_cancel(self, session: RuntimeSession) -> None:
        try:
            session.cancel(self._settings.grace_seconds)
        except Exception as error:  # native cancel is best effort before signals
            self._store.append_event(
                self._agent_id, "native_cancel_failed", data={"error": str(error)}
            )

    def _await_exit(self, session: RuntimeSession) -> Outcome | None:
        started = self._ops.monotonic()
        while self._ops.monotonic() - started < self._settings.grace_seconds:
            outcome = session.wait(self._settings.poll_seconds)
            if outcome is not None:
                return outcome
        return None

    def _finish(
        self, session: RuntimeSession, session_outcome: Outcome | None
    ) -> Outcome:
        """Verify the group is gone and the answer is real before going terminal."""

        assert self._pgid is not None
        termination = terminate_process_group(
            self._ops,
            self._pgid,
            grace_seconds=self._settings.grace_seconds,
            kill_grace_seconds=self._settings.kill_grace_seconds,
            poll_seconds=min(self._settings.poll_seconds, 1.0),
        )
        if termination.signals or not termination.group_gone:
            self._store.append_event(
                self._agent_id,
                "process_group_terminated",
                data={
                    "signals": list(termination.signals),
                    "group_gone": termination.group_gone,
                    "process_group_id": self._pgid,
                },
            )
        if session.pid is not None:
            self._ops.reap(session.pid)
        proof = inspect_answer(self._answer_path, sentinel=self._settings.sentinel)
        outcome = verify_completion(
            session_outcome=session_outcome,
            stop_reason=self._stop_reason,
            answer=proof,
            group_gone=termination.group_gone,
            last_progress_at=self._sink.last_progress_at,
            now=self._ops.monotonic(),
            silence_threshold_seconds=self._settings.silence_threshold_seconds,
        )
        return self._commit(outcome)

    def _commit(self, outcome: Outcome) -> Outcome:
        """Commit terminal state, honouring an already-durable cancel intent."""

        current = AgentStatus(self._store.get_agent(self._agent_id)["status"])
        if current is AgentStatus.CANCELLING and outcome.status is not AgentStatus.CANCELLED:
            outcome = Outcome(
                AgentStatus.CANCELLED,
                exit_code=outcome.exit_code,
                failure_kind=outcome.failure_kind,
                failure_text=outcome.failure_text,
                runtime_session_id=outcome.runtime_session_id,
                answer_path=outcome.answer_path,
                answer_bytes=outcome.answer_bytes,
                answer_sha256=outcome.answer_sha256,
            )
        try:
            self._store.transition(
                self._agent_id, outcome.status, outcome=outcome, kind="terminal"
            )
        except AgentRunError:
            # Another writer already sealed this agent; its record wins.
            pass
        return outcome
