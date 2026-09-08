"""Semantic completion verification: sentinel, answer proof, silence evidence."""

from __future__ import annotations

import codecs
import errno
import hashlib
import json
import math
import os
import stat
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

ANSWER_FORMAT_FILENAME = ".answer-format"
"""Agent-directory marker that makes the current answer format durable."""

ANSWER_FORMAT_CONTENT = b"2\n"
"""Exact contents of the current agent-directory answer-format marker."""

MAX_ANSWER_PAYLOAD_BYTES = 16 * 1024 * 1024
"""Maximum answer payload size accepted by the core read path."""

_MAX_ANSWER_METADATA_BYTES = 4096
"""Maximum bytes read from either answer metadata file."""

_LEGACY_FRAME = b"\n" + DEFAULT_SENTINEL.encode("utf-8") + b"\n"


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


def _open_regular_descriptor(
    path: Path, *, owned_root: Path | None = None
) -> tuple[int, os.stat_result]:
    """Open ``path`` without following symlinks and return its descriptor state.

    When ``owned_root`` is supplied, every relative directory component is
    opened from that root with ``O_NOFOLLOW`` before the final regular file.
    This anchors the open to the owned tree even if names are swapped during
    validation. The caller owns the returned descriptor. Missing and operating
    system failures propagate; escapes and non-regular files raise
    ``PathEscapeError``.
    """

    file_flags = os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW
    directories: list[int] = []
    try:
        if owned_root is None:
            descriptor = os.open(path, file_flags)
        else:
            if not path.is_absolute() or not owned_root.is_absolute():
                raise PathEscapeError("owned answer paths must be absolute")
            try:
                relative = path.relative_to(owned_root)
            except ValueError as error:
                raise PathEscapeError(f"answer artifact escapes owned directory: {path}") from error
            if not relative.parts or ".." in relative.parts:
                raise PathEscapeError(f"answer artifact escapes owned directory: {path}")
            directory_flags = os.O_RDONLY | os.O_CLOEXEC | os.O_DIRECTORY | os.O_NOFOLLOW
            current = os.open(owned_root, directory_flags)
            directories.append(current)
            for part in relative.parts[:-1]:
                current = os.open(part, directory_flags, dir_fd=current)
                directories.append(current)
            descriptor = os.open(relative.name, file_flags, dir_fd=current)
        try:
            metadata = os.fstat(descriptor)
        except BaseException:
            os.close(descriptor)
            raise
        if not stat.S_ISREG(metadata.st_mode):
            os.close(descriptor)
            raise PathEscapeError(f"answer artifact must be a regular file: {path}")
        return descriptor, metadata
    finally:
        for directory in reversed(directories):
            os.close(directory)


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
        """Return whether the artifact has valid completion evidence."""

        if not self.exists or self.size_bytes == 0:
            return False
        if self.proof_version == ANSWER_FORMAT_PROOF:
            return self.proof_error is None
        return self.sentinel_found

    @property
    def evidence(self) -> str:
        """Return the stable completion-evidence label for this artifact."""

        if not self.exists or self.size_bytes == 0:
            return NO_ANSWER
        if self.proof_version == ANSWER_FORMAT_PROOF:
            return ANSWER_PRESENT if self.proof_error is None else ANSWER_INCOMPLETE
        return ANSWER_PRESENT if self.sentinel_found else ANSWER_INCOMPLETE


def answer_proof_path(path: str | Path) -> Path:
    """Return the proof-sidecar path paired with one answer artifact path."""

    answer = Path(path)
    return answer.with_name(f"{answer.name}{ANSWER_PROOF_SUFFIX}")


def answer_format_path(path: str | Path) -> Path:
    """Return the durable format-marker path for one answer artifact."""

    return Path(path).parent / ANSWER_FORMAT_FILENAME


def _read_answer_metadata(
    path: Path, label: str, *, owned_root: Path | None = None
) -> bytes | None:
    """Read one optional no-follow metadata file within the sidecar bound.

    ``path`` names the marker or proof and ``label`` supplies its public error
    name. ``owned_root`` optionally anchors every opened component. Missing
    metadata returns ``None``; links, special files, oversize data, and I/O
    failures raise ``AnswerProofError``. Size and bytes come from one descriptor.
    """

    try:
        descriptor, metadata = _open_regular_descriptor(path, owned_root=owned_root)
    except FileNotFoundError:
        return None
    except PathEscapeError:
        raise AnswerProofError(f"{label} must be a regular file") from None
    except OSError as error:
        if error.errno in {errno.ELOOP, errno.ENOTDIR}:
            raise AnswerProofError(f"{label} must be a regular file") from error
        raise AnswerProofError(f"{label} is unreadable: {error}") from error
    if metadata.st_size > _MAX_ANSWER_METADATA_BYTES:
        os.close(descriptor)
        raise AnswerProofError(
            f"{label} exceeds the {_MAX_ANSWER_METADATA_BYTES}-byte bound"
        )
    try:
        with os.fdopen(descriptor, "rb") as stream:
            data = stream.read(_MAX_ANSWER_METADATA_BYTES + 1)
    except OSError as error:
        raise AnswerProofError(f"{label} is unreadable: {error}") from error
    if len(data) > _MAX_ANSWER_METADATA_BYTES:
        raise AnswerProofError(
            f"{label} exceeds the {_MAX_ANSWER_METADATA_BYTES}-byte bound"
        )
    return data


def inspect_answer(
    path: str | Path,
    *,
    sentinel: str | None = DEFAULT_SENTINEL,
    owned_root: Path | None = None,
) -> AnswerProof:
    """Hash the answer file and establish its completion proof.

    A versioned sidecar written by the current sealer proves completion by
    matching the payload's exact size and hash; a malformed or contradicting
    sidecar leaves the answer incomplete and is never downgraded to legacy
    semantics. Without a sidecar the artifact is historical and completion
    requires the exact terminal sentinel frame. ``owned_root`` optionally
    anchors every file open beneath a trusted directory. Payload reads stop at
    ``MAX_ANSWER_PAYLOAD_BYTES`` and raise ``AnswerOversizedError`` above it.
    """

    if sentinel is not None and (not isinstance(sentinel, str) or not sentinel.strip()):
        raise ValidationError("sentinel must be a nonblank string or None")
    answer = Path(path)
    try:
        descriptor, metadata = _open_regular_descriptor(answer, owned_root=owned_root)
    except FileNotFoundError:
        return AnswerProof(answer, False, 0, None, False)
    except PathEscapeError:
        raise
    except OSError as error:
        if error.errno in {errno.ELOOP, errno.ENOTDIR}:
            raise PathEscapeError(f"answer artifact must be a regular file: {answer}") from error
        raise ValidationError(f"cannot inspect answer file: {error}") from error
    if metadata.st_size > MAX_ANSWER_PAYLOAD_BYTES:
        os.close(descriptor)
        raise AnswerOversizedError(
            f"answer artifact is {metadata.st_size} bytes, above the "
            f"{MAX_ANSWER_PAYLOAD_BYTES}-byte inspection bound"
        )
    digest = hashlib.sha256()
    size = 0
    tail = b""
    frame = None if sentinel is None else b"\n" + sentinel.encode("utf-8") + b"\n"
    try:
        with os.fdopen(descriptor, "rb") as handle:
            while True:
                chunk = handle.read(_CHUNK)
                if not chunk:
                    break
                size += len(chunk)
                if size > MAX_ANSWER_PAYLOAD_BYTES:
                    raise AnswerOversizedError(
                        f"answer artifact exceeds the {MAX_ANSWER_PAYLOAD_BYTES}-byte "
                        "inspection bound"
                    )
                digest.update(chunk)
                if frame is not None:
                    tail = (tail + chunk)[-len(frame) :]
    except AnswerOversizedError:
        raise
    except OSError as error:
        raise ValidationError(f"cannot read answer file: {error}") from error
    found = frame is None or tail == frame
    proof_version, proof_error = _inspect_proof_sidecar(
        answer, size, digest.hexdigest(), owned_root=owned_root
    )
    return AnswerProof(
        answer,
        True,
        size,
        digest.hexdigest(),
        bool(found and size > 0),
        proof_version=proof_version,
        proof_error=proof_error,
    )


def _inspect_proof_sidecar(
    answer: Path,
    size: int,
    sha256: str,
    *,
    owned_root: Path | None = None,
) -> tuple[int, str | None]:
    """Classify one hashed payload's proof sidecar without raising.

    The durable directory marker pins new artifacts to the proof format even
    when their required proof is missing or corrupt. Only directories with
    neither marker nor proof are treated as historical sentinel format.
    """

    try:
        version, _ = _load_answer_proof(answer, size, sha256, owned_root=owned_root)
    except AnswerProofError as error:
        return ANSWER_FORMAT_PROOF, str(error)
    return version, None


def _load_answer_proof(
    answer: Path,
    size: int,
    sha256: str,
    *,
    owned_root: Path | None = None,
) -> tuple[int, dict[str, object] | None]:
    """Load one proof with optional owned-root anchoring and verify its payload."""

    marker = _read_answer_metadata(
        answer_format_path(answer), "answer format marker", owned_root=owned_root
    )
    raw = _read_answer_metadata(
        answer_proof_path(answer), "answer proof sidecar", owned_root=owned_root
    )
    if marker is None and raw is None:
        return ANSWER_FORMAT_LEGACY, None
    if marker is not None and marker != ANSWER_FORMAT_CONTENT:
        raise AnswerProofError("answer format marker is malformed")
    if raw is None:
        raise AnswerProofError("answer proof sidecar is missing")
    try:
        proof = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise AnswerProofError(f"answer proof sidecar is malformed: {error}") from error
    if not isinstance(proof, dict):
        raise AnswerProofError("answer proof sidecar must contain a JSON object")
    problem = _proof_mismatch(proof, answer.name, size, sha256)
    if problem is not None:
        raise AnswerProofError(problem)
    return ANSWER_FORMAT_PROOF, proof


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
    path: str | Path,
    *,
    expected_bytes: int,
    expected_sha256: str,
    owned_root: Path | None = None,
) -> dict | None:
    """Verify the proof sidecar for one stored answer, or ``None`` if legacy.

    An agent-directory format marker makes the current proof mandatory. Only
    directories with neither marker nor proof use historical legacy handling.
    ``owned_root`` optionally anchors all metadata opens beneath a trusted tree.
    Unreadable, oversized, malformed, or contradicting metadata raises
    ``AnswerProofError`` and never silently downgrades a new-format payload.
    """

    _, proof = _load_answer_proof(
        Path(path), expected_bytes, expected_sha256, owned_root=owned_root
    )
    return proof


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
    owned_root: Path | None = None,
    return_content: bool = True,
) -> str | None:
    """Read one owned answer artifact in full against its recorded proof.

    The read is bounded by ``max_bytes``, independently of any inline display
    cutoff, so valid payloads larger than the inline limit still reach
    validation. ``expected_bytes``/``expected_sha256`` are the durable values
    recorded at seal time; ``strip_legacy`` removes the exact historical
    terminal sentinel frame once for ``ANSWER_FORMAT_LEGACY`` artifacts. The
    path must be a regular file, never a symlink or special file.
    ``owned_root`` optionally anchors all path components. When
    ``return_content`` is false, bytes are still hashed and incrementally
    UTF-8 validated but decoded text is discarded instead of accumulated. Raises
    ``AnswerMissingError``, ``AnswerOversizedError``, ``AnswerTamperedError``,
    ``AnswerEncodingError``, or ``PathEscapeError`` with the failing cause.
    """

    if isinstance(max_bytes, bool) or not isinstance(max_bytes, int) or max_bytes < 1:
        raise ValidationError("max_bytes must be a positive integer")
    answer = Path(path)
    try:
        descriptor, metadata = _open_regular_descriptor(answer, owned_root=owned_root)
    except FileNotFoundError:
        raise AnswerMissingError(f"answer artifact is missing: {answer}") from None
    except PathEscapeError:
        raise
    except OSError as error:
        if error.errno in {errno.ELOOP, errno.ENOTDIR}:
            raise PathEscapeError(f"answer artifact must be a regular file: {answer}") from error
        raise ValidationError(f"cannot stat answer artifact: {error}") from error
    size = metadata.st_size
    if size != expected_bytes:
        os.close(descriptor)
        raise AnswerTamperedError("answer artifact size does not match its recorded proof")
    if size > max_bytes:
        os.close(descriptor)
        raise AnswerOversizedError(
            f"answer artifact is {size} bytes, above the {max_bytes}-byte read bound"
        )
    digest = hashlib.sha256()
    decoder = codecs.getincrementaldecoder("utf-8")()
    decoded: list[str] | None = [] if return_content else None
    encoding_error: UnicodeDecodeError | None = None
    seen = 0
    try:
        with os.fdopen(descriptor, "rb") as handle:
            while chunk := handle.read(_CHUNK):
                seen += len(chunk)
                digest.update(chunk)
                if encoding_error is None:
                    try:
                        text = decoder.decode(chunk, final=False)
                    except UnicodeDecodeError as error:
                        encoding_error = error
                    else:
                        if decoded is not None:
                            decoded.append(text)
    except OSError as error:
        raise ValidationError(f"cannot read answer artifact: {error}") from error
    if seen != expected_bytes:
        raise AnswerTamperedError("answer artifact size does not match its recorded proof")
    if digest.hexdigest() != expected_sha256:
        raise AnswerTamperedError("answer artifact hash does not match its recorded proof")
    if encoding_error is None:
        try:
            final = decoder.decode(b"", final=True)
        except UnicodeDecodeError as error:
            encoding_error = error
        else:
            if decoded is not None:
                decoded.append(final)
    if encoding_error is not None:
        raise AnswerEncodingError("answer artifact is not valid UTF-8") from encoding_error
    if decoded is None:
        return None
    payload = "".join(decoded)
    frame = _LEGACY_FRAME.decode("utf-8")
    if strip_legacy and payload.endswith(frame):
        payload = payload[: -len(frame)]
    return payload


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
