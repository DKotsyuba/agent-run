"""Runtime snapshot-index finalization and recovery inspection."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path, PurePosixPath

from ..errors import PathEscapeError, ValidationError
from .home import content_hash, write_managed_file
from .snapshot_tree import (
    RUNTIME_SNAPSHOT_INDEX,
    _MAX_SNAPSHOT_METADATA_BYTES,
    _entry_map,
    _read_metadata,
    _read_tree,
    inspect_managed_snapshot,
)


@dataclass(frozen=True, slots=True)
class RuntimeSnapshotInspection:
    """Aggregate recovery result for every indexed managed runtime artifact."""

    verified: bool
    missing: tuple[str, ...]
    mismatched: tuple[str, ...]
    owned_temps: tuple[str, ...]
    orphans: tuple[str, ...]


def finalize_runtime_snapshots(
    home: Path,
    materialize_revision: str,
    managed_files: tuple[str, ...] = (),
) -> str:
    """Finalize the producer index and return its exact SHA-256.

    ``materialize_revision`` binds the adapter result to every registered tree.
    ``managed_files`` adds adapter-known flat config files through the same
    no-follow tree reader. No live source paths or native runtime state are read.
    """

    if not isinstance(materialize_revision, str) or not materialize_revision.strip():
        raise ValidationError("materialize revision must be nonblank")
    try:
        payload = _read_metadata(home, RUNTIME_SNAPSHOT_INDEX)
    except FileNotFoundError:
        roots: list[str] = []
    else:
        try:
            roots = json.loads(payload)["roots"]
        except (UnicodeDecodeError, json.JSONDecodeError, KeyError, TypeError) as error:
            raise ValidationError("runtime snapshot index is malformed") from error
        if not isinstance(roots, list) or any(not isinstance(root, str) for root in roots):
            raise ValidationError("runtime snapshot index is malformed")
    files: list[dict[str, object]] = []
    for value in managed_files:
        path = PurePosixPath(value)
        if path.is_absolute() or "." in path.parts or ".." in path.parts:
            raise PathEscapeError(f"managed runtime file escapes home: {value}")
        entries, _ = _read_tree(home, (path,))
        files.append(next(entry for entry in entries if entry["path"] == value))
    payload = (
        json.dumps(
            {
                "snapshot_index_version": 1,
                "materialize_revision": materialize_revision,
                "roots": sorted(roots),
                "files": sorted(files, key=lambda entry: str(entry["path"])),
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        + b"\n"
    )
    if len(payload) > _MAX_SNAPSHOT_METADATA_BYTES:
        raise ValidationError("runtime snapshot index exceeds the metadata bound")
    write_managed_file(home, RUNTIME_SNAPSHOT_INDEX, payload)
    return content_hash(payload)


def runtime_snapshot_index_sha256(home: Path, expected_revision: str) -> str:
    """Return the freshly finalized index hash for one trusted materialization.

    This synchronous producer-side binding validates the no-follow bounded index
    and its materialize revision. Resume must instead use
    :func:`inspect_runtime_snapshots` with the previously stored hash.
    """

    raw = _read_metadata(home, RUNTIME_SNAPSHOT_INDEX)
    try:
        document = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValidationError("runtime snapshot index is malformed") from error
    if not isinstance(document, dict) or document.get("materialize_revision") != expected_revision:
        raise ValidationError("runtime snapshot index revision does not match materialization")
    return content_hash(raw)


def _valid_sha256(value: object) -> bool:
    """Return whether ``value`` is one lowercase or uppercase SHA-256 hex string."""

    if not isinstance(value, str) or len(value) != 64:
        return False
    try:
        int(value, 16)
    except ValueError:
        return False
    return True


def inspect_runtime_snapshots(
    home: Path, expected_revision: str, *, expected_sha256: str
) -> RuntimeSnapshotInspection:
    """Verify the bound producer index, roots, and flat managed config files."""

    if not isinstance(expected_revision, str) or not expected_revision.strip():
        raise ValidationError("expected materialize revision must be nonblank")
    if not _valid_sha256(expected_sha256):
        raise ValidationError("runtime snapshot index sha256 is invalid")
    try:
        raw = _read_metadata(home, RUNTIME_SNAPSHOT_INDEX)
        document = json.loads(raw)
        roots = document["roots"]
        files = document["files"]
    except FileNotFoundError as error:
        raise ValidationError("runtime snapshot index is missing") from error
    except (UnicodeDecodeError, json.JSONDecodeError, KeyError, TypeError) as error:
        raise ValidationError("runtime snapshot index is malformed") from error
    if content_hash(raw) != expected_sha256:
        raise ValidationError("runtime snapshot index hash does not match config snapshot")
    if document.get("materialize_revision") != expected_revision:
        raise ValidationError("runtime snapshot index revision does not match config snapshot")
    if (
        document.get("snapshot_index_version") != 1
        or not isinstance(roots, list)
        or not isinstance(files, list)
    ):
        raise ValidationError("runtime snapshot index is malformed")
    if any(not isinstance(root, str) for root in roots) or roots != sorted(set(roots)):
        raise ValidationError("runtime snapshot index roots are malformed")
    missing: list[str] = []
    mismatched: list[str] = []
    temps: list[str] = []
    orphans: list[str] = []
    for root in roots:
        inspection = inspect_managed_snapshot(home, root)
        missing.extend(f"{root}/{path}" for path in inspection.referenced_missing)
        mismatched.extend(f"{root}/{path}" for path in inspection.mismatched)
        temps.extend(f"{root}/{path}" for path in inspection.owned_temps)
        orphans.extend(f"{root}/{path}" for path in inspection.orphans)
    expected_files = _entry_map(files)
    for path, expected in expected_files.items():
        try:
            entries, _ = _read_tree(home, (PurePosixPath(path),))
            actual = next(entry for entry in entries if entry["path"] == path)
        except (ValidationError, StopIteration):
            missing.append(path)
        else:
            if actual != expected:
                mismatched.append(path)
    return RuntimeSnapshotInspection(
        not (missing or mismatched or temps or orphans),
        tuple(sorted(missing)),
        tuple(sorted(mismatched)),
        tuple(sorted(temps)),
        tuple(sorted(orphans)),
    )

