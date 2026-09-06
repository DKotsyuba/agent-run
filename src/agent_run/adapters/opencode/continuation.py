"""OpenCode continuation boundaries and immutable per-run answer persistence."""

from dataclasses import replace
from pathlib import Path
from typing import Mapping, Sequence

from ...domain import Outcome
from ...errors import ValidationError
from ..home import seal_answer
from .normalize import PRIMARY_AGENT, extract_answer

ANSWER_NAME = "answer.md"
"""Per-run answer filename; the directory is unique to the durable agent ID."""


def seal_result(outcome: Outcome, payload: Mapping[str, object] | Sequence[object], directory: Path | None, agent: str) -> Outcome:
    """Attach proof for this turn's answer, leaving text-less outcomes unchanged.

    Only the caller's per-run directory is written; earlier run artifacts are
    never selected. Filesystem errors propagate instead of inventing proof.
    """
    answer = extract_answer(payload, agent=agent)
    if not answer or directory is None:
        return outcome
    path = Path(directory) / ANSWER_NAME
    size, digest = seal_answer(path, answer)
    return replace(outcome, answer_path=path, answer_bytes=size, answer_sha256=digest)


def message_entries(payload: object) -> list[Mapping[str, object]]:
    """Read the native flat message list, refusing entries without stable IDs."""
    entries = payload.get("data") if isinstance(payload, Mapping) else payload
    if not isinstance(entries, (list, tuple)):
        raise ValidationError("opencode resume transcript must contain a message list")
    if any(not isinstance(item, Mapping) or not isinstance(item.get("id"), str) or not item["id"] for item in entries):
        raise ValidationError("opencode resume transcript contains an unidentified message")
    return list(entries)


def resume_boundary(client, identity: str, workdir: str, model: Mapping) -> frozenset[str]:
    """Prove the owned service has the same idle session and capture old message IDs.

    The caller has already verified the service descriptor. Missing identity,
    location, selected model/agent or an active session raises ValidationError;
    this function never creates, prompts or changes a native session.
    """
    info = client.session_info(identity)
    location = info.get("location", {})
    current_model = info.get("model", {})
    if (
        info.get("id") != identity or info.get("agent") != PRIMARY_AGENT
        or not isinstance(location, Mapping) or not isinstance(location.get("directory"), str)
        or Path(location["directory"]).resolve() != Path(workdir).resolve()
        or not isinstance(current_model, Mapping)
        or any(current_model.get(key) != value for key, value in model.items())
    ):
        raise ValidationError("opencode saved session identity, model or workdir does not match")
    statuses = client.session_status()
    if not isinstance(statuses, Mapping):
        raise ValidationError("opencode session status must be a mapping")
    if identity in statuses:
        raise ValidationError("opencode saved session is active or its state is unprovable")
    capture = client.messages(identity)
    try:
        return frozenset(str(item["id"]) for item in message_entries(capture.json()))
    finally:
        capture.release()
