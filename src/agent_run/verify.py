"""Semantic completion verification: sentinel, answer proof, silence evidence."""

from __future__ import annotations

import hashlib
import math
from dataclasses import dataclass
from pathlib import Path

from .domain import AgentStatus, Outcome
from .errors import ValidationError


DEFAULT_SENTINEL = "<<<agent-run:complete>>>"

NO_ANSWER = "no_answer"
ANSWER_INCOMPLETE = "answer_incomplete"
ANSWER_PRESENT = "answer_present"
ENGINE_VANISHED = "engine_vanished"
GROUP_SURVIVED = "engine_group_survived"

STOP_CANCEL = "cancel"
STOP_TIMEOUT = "timeout"

_CHUNK = 65536


@dataclass(frozen=True)
class AnswerProof:
    """What is on disk for an agent's answer, and whether it terminated."""

    path: Path
    exists: bool
    size_bytes: int
    sha256: str | None
    sentinel_found: bool

    @property
    def complete(self) -> bool:
        return self.exists and self.size_bytes > 0 and self.sentinel_found

    @property
    def evidence(self) -> str:
        if not self.exists or self.size_bytes == 0:
            return NO_ANSWER
        return ANSWER_PRESENT if self.sentinel_found else ANSWER_INCOMPLETE


def inspect_answer(path: str | Path, *, sentinel: str | None = DEFAULT_SENTINEL) -> AnswerProof:
    """Hash the answer file and look for the completion sentinel.

    A file without its sentinel is an answer cut off during write, which is a
    different failure from no answer at all.
    """

    if sentinel is not None and (not isinstance(sentinel, str) or not sentinel.strip()):
        raise ValidationError("sentinel must be a nonblank string or None")
    answer = Path(path)
    digest = hashlib.sha256()
    size = 0
    tail = b""
    marker = None if sentinel is None else sentinel.encode("utf-8")
    found = sentinel is None
    try:
        with answer.open("rb") as handle:
            while True:
                chunk = handle.read(_CHUNK)
                if not chunk:
                    break
                size += len(chunk)
                digest.update(chunk)
                if marker is not None and not found:
                    window = tail + chunk
                    found = marker in window
                    tail = window[-(len(marker) - 1) :] if len(marker) > 1 else b""
    except FileNotFoundError:
        return AnswerProof(answer, False, 0, None, False)
    except OSError as error:
        raise ValidationError(f"cannot read answer file: {error}") from error
    return AnswerProof(answer, True, size, digest.hexdigest(), bool(found and size > 0))


def silence_seconds(last_progress_at: float | None, now: float) -> float | None:
    """Seconds since the last observed engine progress, or None if never any."""

    if last_progress_at is None:
        return None
    for name, value in (("last_progress_at", last_progress_at), ("now", now)):
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ValidationError(f"{name} must be a finite number")
        if not math.isfinite(value):
            raise ValidationError(f"{name} must be a finite number")
    return max(0.0, float(now) - float(last_progress_at))


def _silence_note(last_progress_at: float | None, now: float, threshold: float) -> str:
    quiet = silence_seconds(last_progress_at, now)
    if quiet is None:
        return "silence=no_progress"
    label = "silent" if quiet >= threshold else "active"
    return f"silence={quiet:.1f}s/{label}"


def _answered(proof: AnswerProof | None, outcome: Outcome | None) -> Outcome:
    if proof is None or not proof.complete:
        return Outcome(
            AgentStatus.SUCCEEDED,
            exit_code=None if outcome is None else outcome.exit_code,
            runtime_session_id=None if outcome is None else outcome.runtime_session_id,
        )
    return Outcome(
        AgentStatus.SUCCEEDED,
        exit_code=None if outcome is None else outcome.exit_code,
        runtime_session_id=None if outcome is None else outcome.runtime_session_id,
        answer_path=proof.path,
        answer_bytes=proof.size_bytes,
        answer_sha256=proof.sha256,
    )


def verify_completion(
    *,
    session_outcome: Outcome | None,
    stop_reason: str | None,
    answer: AnswerProof | None,
    group_gone: bool,
    last_progress_at: float | None = None,
    now: float = 0.0,
    silence_threshold_seconds: float = 60.0,
) -> Outcome:
    """Decide the terminal outcome from process facts plus answer evidence.

    The engine's own exit status is never trusted on its own: a success without
    a complete answer is a failure, and no terminal state is issued while the
    engine process group is still alive.
    """

    if stop_reason is not None and stop_reason not in {STOP_CANCEL, STOP_TIMEOUT}:
        raise ValidationError("stop_reason must be cancel, timeout, or None")
    if session_outcome is not None and not isinstance(session_outcome, Outcome):
        raise ValidationError("session_outcome must be an Outcome or None")
    if answer is not None and not isinstance(answer, AnswerProof):
        raise ValidationError("answer must be an AnswerProof or None")
    note = _silence_note(last_progress_at, now, silence_threshold_seconds)
    evidence = NO_ANSWER if answer is None else answer.evidence
    session_id = None if session_outcome is None else session_outcome.runtime_session_id

    if not group_gone:
        return Outcome(
            AgentStatus.FAILED,
            failure_kind=GROUP_SURVIVED,
            failure_text=f"{evidence}; {note}",
            runtime_session_id=session_id,
        )
    if stop_reason == STOP_CANCEL:
        return Outcome(
            AgentStatus.CANCELLED,
            failure_kind=evidence,
            failure_text=note,
            runtime_session_id=session_id,
        )
    if stop_reason == STOP_TIMEOUT:
        return Outcome(
            AgentStatus.TIMED_OUT,
            failure_kind=evidence,
            failure_text=note,
            runtime_session_id=session_id,
        )
    if session_outcome is None:
        return Outcome(
            AgentStatus.FAILED,
            failure_kind=ENGINE_VANISHED,
            failure_text=f"{evidence}; {note}",
        )
    if session_outcome.status is not AgentStatus.SUCCEEDED:
        return session_outcome
    if answer is None or not answer.complete:
        return Outcome(
            AgentStatus.FAILED,
            exit_code=session_outcome.exit_code,
            failure_kind=evidence,
            failure_text=note,
            runtime_session_id=session_id,
        )
    return _answered(answer, session_outcome)
