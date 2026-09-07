"""Semantic completion verification: sentinel, answer proof, silence evidence."""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass
from pathlib import Path

from .domain import AgentStatus, Outcome
from .errors import PathEscapeError, ValidationError


DEFAULT_SENTINEL = "<<<agent-run:complete>>>"

NO_ANSWER = "no_answer"
ANSWER_INCOMPLETE = "answer_incomplete"
ANSWER_PRESENT = "answer_present"
ENGINE_VANISHED = "engine_vanished"
GROUP_SURVIVED = "engine_group_survived"

STOP_CANCEL = "cancel"
STOP_TIMEOUT = "timeout"

_CHUNK = 65536

ANSWER_KIND = "agent_answer"
"""Stable ``kind`` label carried by every sealed-answer descriptor and proof."""

ANSWER_MEDIA_TYPE = "text/markdown; charset=utf-8"
"""Media type of sealed answer payloads: engine result text, always UTF-8."""

ANSWER_FORMAT_LEGACY = 1
"""Proof version of historical artifacts: payload plus a terminal sentinel frame."""

ANSWER_FORMAT_PROOF = 2
"""Proof version of clean artifacts: exact payload bytes plus a proof sidecar."""

ANSWER_PROOF_SUFFIX = ".proof.json"
"""Filename suffix of the versioned proof sidecar written beside each payload."""


class AnswerError(ValidationError):
    """Base class for typed answer-artifact read and proof failures."""


class AnswerMissingError(AnswerError):
    """The stored answer artifact is absent from the filesystem."""


class AnswerTamperedError(AnswerError):
    """The stored answer artifact no longer matches its recorded size or hash."""


class AnswerOversizedError(AnswerError):
    """The stored answer artifact exceeds the caller's independent read bound."""


class AnswerEncodingError(AnswerError):
    """The stored answer artifact is not valid UTF-8."""


class AnswerProofError(AnswerError):
    """The versioned proof sidecar is malformed or contradicts the payload."""


@dataclass(frozen=True)
class AnswerProof:
    """What is on disk for an agent's answer, and whether it terminated.

    ``proof_version`` names the explicit on-disk format:
    ``ANSWER_FORMAT_PROOF`` when a versioned sidecar proves the payload's
    exact bytes, ``ANSWER_FORMAT_LEGACY`` for historical sentinel-framed
    artifacts. ``proof_error`` records why a sidecar proof failed; a failed
    proof keeps the artifact incomplete and is never downgraded to legacy
    sentinel semantics.
    """

    path: Path
    exists: bool
    size_bytes: int
    sha256: str | None
    sentinel_found: bool
    proof_version: int = ANSWER_FORMAT_LEGACY
    proof_error: str | None = None

    @property
    def complete(self) -> bool:
        if not self.exists or self.size_bytes == 0:
            return False
        if self.proof_version == ANSWER_FORMAT_PROOF:
            return self.proof_error is None
        return self.sentinel_found

    @property
    def evidence(self) -> str:
        if not self.exists or self.size_bytes == 0:
            return NO_ANSWER
        if self.proof_version == ANSWER_FORMAT_PROOF:
            return ANSWER_PRESENT if self.proof_error is None else ANSWER_INCOMPLETE
        return ANSWER_PRESENT if self.sentinel_found else ANSWER_INCOMPLETE


def answer_proof_path(path: str | Path) -> Path:
    """Return the proof-sidecar path paired with one answer artifact path."""

    answer = Path(path)
    return answer.with_name(f"{answer.name}{ANSWER_PROOF_SUFFIX}")


def inspect_answer(path: str | Path, *, sentinel: str | None = DEFAULT_SENTINEL) -> AnswerProof:
    """Hash the answer file and establish its completion proof.

    A versioned sidecar written by the current sealer proves completion by
    matching the payload's exact size and hash; a malformed or contradicting
    sidecar leaves the answer incomplete and is never downgraded to legacy
    semantics. Without a sidecar the artifact is historical and completion
    requires the terminal sentinel, whose absence marks an answer cut off
    during write -- a different failure from no answer at all.
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
    proof_version, proof_error = _inspect_proof_sidecar(answer, size, digest.hexdigest())
    return AnswerProof(
        answer,
        True,
        size,
        digest.hexdigest(),
        bool(found and size > 0),
        proof_version=proof_version,
        proof_error=proof_error,
    )


def _inspect_proof_sidecar(answer: Path, size: int, sha256: str) -> tuple[int, str | None]:
    """Classify one hashed payload's proof sidecar without raising.

    A missing sidecar marks the legacy sentinel format; a present sidecar
    pins the sidecar format and yields the payload mismatch reason, if any.
    """

    try:
        raw = answer_proof_path(answer).read_bytes()
    except FileNotFoundError:
        return ANSWER_FORMAT_LEGACY, None
    except OSError as error:
        return ANSWER_FORMAT_PROOF, f"answer proof sidecar is unreadable: {error}"
    try:
        proof = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        return ANSWER_FORMAT_PROOF, f"answer proof sidecar is malformed: {error}"
    return ANSWER_FORMAT_PROOF, _proof_mismatch(proof, answer.name, size, sha256)


def _proof_mismatch(proof: object, answer_name: str, size: int, sha256: str) -> str | None:
    """Return why one decoded proof mapping contradicts its payload, if it does."""

    if not isinstance(proof, dict):
        return "answer proof sidecar must contain a JSON object"
    version = proof.get("proof_version")
    if isinstance(version, bool) or version != ANSWER_FORMAT_PROOF:
        return f"answer proof version is unsupported: {version!r}"
    if proof.get("kind") != ANSWER_KIND:
        return f"answer proof kind must be {ANSWER_KIND!r}"
    if proof.get("media_type") != ANSWER_MEDIA_TYPE:
        return f"answer proof media_type must be {ANSWER_MEDIA_TYPE!r}"
    if proof.get("answer") != answer_name:
        return "answer proof does not name its payload file"
    declared = proof.get("bytes")
    if isinstance(declared, bool) or not isinstance(declared, int) or declared != size:
        return "answer proof byte count does not match the payload"
    if proof.get("sha256") != sha256:
        return "answer proof hash does not match the payload"
    return None


def answer_proof_document(answer_name: str, size: int, sha256: str) -> bytes:
    """Serialize the versioned proof sidecar for one freshly sealed payload.

    The proof binds the payload's file name, exact byte count, and SHA-256
    under ``ANSWER_FORMAT_PROOF`` with the stable kind and media type.
    """

    document = {
        "kind": ANSWER_KIND,
        "media_type": ANSWER_MEDIA_TYPE,
        "proof_version": ANSWER_FORMAT_PROOF,
        "answer": answer_name,
        "bytes": size,
        "sha256": sha256,
    }
    return json.dumps(document, sort_keys=True, separators=(",", ":")).encode("utf-8") + b"\n"


def load_answer_proof(
    path: str | Path, *, expected_bytes: int, expected_sha256: str
) -> dict | None:
    """Verify the proof sidecar for one stored answer, or ``None`` if legacy.

    A missing sidecar marks the historical sentinel format. A present
    sidecar that is unreadable, malformed, or contradicts the recorded
    ``expected_bytes``/``expected_sha256`` raises ``AnswerProofError``; a
    broken new-format proof is never silently downgraded to legacy handling.
    """

    answer = Path(path)
    try:
        raw = answer_proof_path(answer).read_bytes()
    except FileNotFoundError:
        return None
    except OSError as error:
        raise AnswerProofError(f"answer proof sidecar is unreadable: {error}") from error
    try:
        proof = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise AnswerProofError(f"answer proof sidecar is malformed: {error}") from error
    if not isinstance(proof, dict):
        raise AnswerProofError("answer proof sidecar must contain a JSON object")
    problem = _proof_mismatch(proof, answer.name, expected_bytes, expected_sha256)
    if problem is not None:
        raise AnswerProofError(problem)
    return proof


_LEGACY_FRAME = b"\n" + DEFAULT_SENTINEL.encode("utf-8") + b"\n"


def strip_legacy_frame(data: bytes) -> bytes:
    """Remove exactly one known legacy terminal frame from framed bytes.

    The historical sealer wrote ``payload + separator + sentinel + newline``
    where ``separator`` was empty when the payload already ended with a
    newline and a single newline otherwise, so every historical artifact ends
    with the frame ``newline + sentinel + newline``. Only that exact terminal
    frame is removed, once; a sentinel earlier in the body is payload content
    and is preserved. When the original payload itself ended with a newline,
    that newline is not distinguishable from the joining separator, so inline
    presentation may lose one original trailing newline; the recorded size
    and hash always describe the untouched original artifact regardless.
    """

    if data.endswith(_LEGACY_FRAME):
        return data[: -len(_LEGACY_FRAME)]
    return data


def read_answer_payload(
    path: str | Path,
    *,
    expected_bytes: int,
    expected_sha256: str,
    max_bytes: int,
    strip_legacy: bool,
) -> str:
    """Read one owned answer artifact in full against its recorded proof.

    The read is bounded by ``max_bytes``, independently of any inline display
    cutoff, so valid payloads larger than the inline limit still reach
    validation. ``expected_bytes``/``expected_sha256`` are the durable values
    recorded at seal time; ``strip_legacy`` removes the exact historical
    terminal sentinel frame once for ``ANSWER_FORMAT_LEGACY`` artifacts. The
    path must be a regular file, never a symlink or special file. Raises
    ``AnswerMissingError``, ``AnswerOversizedError``, ``AnswerTamperedError``,
    ``AnswerEncodingError``, or ``PathEscapeError`` with the failing cause.
    """

    if isinstance(max_bytes, bool) or not isinstance(max_bytes, int) or max_bytes < 1:
        raise ValidationError("max_bytes must be a positive integer")
    answer = Path(path)
    if answer.is_symlink():
        raise PathEscapeError(f"answer artifact must not be a symlink: {answer}")
    try:
        size = answer.stat().st_size
    except FileNotFoundError:
        raise AnswerMissingError(f"answer artifact is missing: {answer}") from None
    except OSError as error:
        raise ValidationError(f"cannot stat answer artifact: {error}") from error
    if not answer.is_file():
        raise PathEscapeError(f"answer artifact must be a regular file: {answer}")
    if size != expected_bytes:
        raise AnswerTamperedError("answer artifact size does not match its recorded proof")
    if size > max_bytes:
        raise AnswerOversizedError(
            f"answer artifact is {size} bytes, above the {max_bytes}-byte read bound"
        )
    digest = hashlib.sha256()
    content = bytearray()
    try:
        with answer.open("rb") as handle:
            while chunk := handle.read(_CHUNK):
                digest.update(chunk)
                content.extend(chunk)
    except FileNotFoundError:
        raise AnswerMissingError(f"answer artifact is missing: {answer}") from None
    except OSError as error:
        raise ValidationError(f"cannot read answer artifact: {error}") from error
    if digest.hexdigest() != expected_sha256:
        raise AnswerTamperedError("answer artifact hash does not match its recorded proof")
    payload = bytes(content)
    if strip_legacy:
        payload = strip_legacy_frame(payload)
    try:
        return payload.decode("utf-8")
    except UnicodeDecodeError as error:
        raise AnswerEncodingError("answer artifact is not valid UTF-8") from error


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


def _with_answer(outcome: Outcome, proof: AnswerProof | None) -> Outcome:
    if proof is None or not proof.complete:
        return outcome
    return Outcome(
        outcome.status,
        exit_code=outcome.exit_code,
        failure_kind=outcome.failure_kind,
        failure_text=outcome.failure_text,
        runtime_session_id=outcome.runtime_session_id,
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
        return _with_answer(
            Outcome(
                AgentStatus.FAILED,
                failure_kind=GROUP_SURVIVED,
                failure_text=f"{evidence}; {note}",
                runtime_session_id=session_id,
            ),
            answer,
        )
    if stop_reason == STOP_CANCEL:
        return _with_answer(
            Outcome(
                AgentStatus.CANCELLED,
                failure_kind=evidence,
                failure_text=note,
                runtime_session_id=session_id,
            ),
            answer,
        )
    if stop_reason == STOP_TIMEOUT:
        return _with_answer(
            Outcome(
                AgentStatus.TIMED_OUT,
                failure_kind=evidence,
                failure_text=note,
                runtime_session_id=session_id,
            ),
            answer,
        )
    if session_outcome is None:
        return _with_answer(
            Outcome(
                AgentStatus.FAILED,
                failure_kind=ENGINE_VANISHED,
                failure_text=f"{evidence}; {note}",
            ),
            answer,
        )
    if session_outcome.status is not AgentStatus.SUCCEEDED:
        return _with_answer(session_outcome, answer)
    if answer is None or not answer.complete:
        return Outcome(
            AgentStatus.FAILED,
            exit_code=session_outcome.exit_code,
            failure_kind=evidence,
            failure_text=note,
            runtime_session_id=session_id,
        )
    return _with_answer(session_outcome, answer)
