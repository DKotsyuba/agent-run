"""Runtime snapshot-index finalization and recovery inspection."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path, PurePosixPath

from ..errors import PathEscapeError, ValidationError
from .home import (
    content_hash,
    managed_entry_type,
    read_managed_symlink,
    write_managed_file,
)
from .snapshot_tree import (
    RUNTIME_SNAPSHOT_INDEX,
    SNAPSHOT_MANIFEST,
    _MAX_SNAPSHOT_METADATA_BYTES,
    _entry_map,
    _read_metadata,
    _read_tree,
    inspect_managed_snapshot,
)


@dataclass(frozen=True, slots=True)
class RuntimeSnapshotInspection:
    """Aggregate recovery result for every indexed managed runtime artifact.

    ``mismatched`` remains the compatibility aggregate. ``type_mismatches`` and
    ``hash_mismatches`` retain actionable causes; missing and orphan entries are
    the distinct expected and unexpected topology categories.
    """

    verified: bool
    missing: tuple[str, ...]
    mismatched: tuple[str, ...]
    owned_temps: tuple[str, ...]
    orphans: tuple[str, ...]
    type_mismatches: tuple[str, ...] = ()
    hash_mismatches: tuple[str, ...] = ()


def finalize_runtime_snapshots(
    home: Path,
    materialize_revision: str,
    managed_files: tuple[str, ...] = (),
    managed_links: tuple[tuple[str, str], ...] = (),
) -> str:
    """Finalize the producer index and return its exact SHA-256.

    ``materialize_revision`` binds the adapter result to every registered tree.
    ``managed_files`` adds adapter-known flat config files through the same
    no-follow tree reader. ``managed_links`` binds exact declared symlink paths
    and targets without reading target content. No native runtime state is read.
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
    roots = sorted(roots)
    if roots != sorted(set(roots)):
        raise ValidationError("runtime snapshot index roots are malformed")
    manifests: dict[str, str] = {}
    for root in roots:
        inspection = inspect_managed_snapshot(home, root)
        if not inspection.verified:
            raise ValidationError(f"managed snapshot is not verified: {root}")
        manifests[root] = content_hash(
            _read_metadata(Path(home) / root, SNAPSHOT_MANIFEST)
        )
    files: list[dict[str, object]] = []
    for value in managed_files:
        path = PurePosixPath(value)
        if path.is_absolute() or "." in path.parts or ".." in path.parts:
            raise PathEscapeError(f"managed runtime file escapes home: {value}")
        entries, _ = _read_tree(home, (path,))
        files.append(next(entry for entry in entries if entry["path"] == value))
    links: list[dict[str, str]] = []
    for path, target in managed_links:
        portable = PurePosixPath(path)
        if portable.is_absolute() or "." in portable.parts or ".." in portable.parts:
            raise PathEscapeError(f"managed runtime link escapes home: {path}")
        if not isinstance(target, str) or not target:
            raise ValidationError(f"managed runtime link target is invalid: {path}")
        if read_managed_symlink(home, path) != target:
            raise ValidationError(f"managed runtime link target does not match: {path}")
        links.append({"path": path, "target": target})
    payload = (
        json.dumps(
            {
                "snapshot_index_version": 1,
                "materialize_revision": materialize_revision,
                "roots": roots,
                "manifests": manifests,
                "files": sorted(files, key=lambda entry: str(entry["path"])),
                "links": sorted(links, key=lambda entry: entry["path"]),
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
    digest = content_hash(raw)
    inspection = inspect_runtime_snapshots(
        home, expected_revision, expected_sha256=digest
    )
    if not inspection.verified:
        raise ValidationError("runtime snapshot index references unverified artifacts")
    return digest


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
        manifests = document["manifests"]
        files = document["files"]
        links = document["links"]
    except FileNotFoundError as error:
        raise ValidationError("runtime snapshot index is missing") from error
    except (UnicodeDecodeError, json.JSONDecodeError, KeyError, TypeError) as error:
        raise ValidationError("runtime snapshot index is malformed") from error
    if content_hash(raw) != expected_sha256:
        raise ValidationError("runtime snapshot index hash does not match config snapshot")
    if document.get("materialize_revision") != expected_revision:
        raise ValidationError("runtime snapshot index revision does not match config snapshot")
    required = {
        "snapshot_index_version", "materialize_revision", "roots",
        "manifests", "files", "links",
    }
    canonical = json.dumps(document, sort_keys=True, separators=(",", ":")).encode("utf-8") + b"\n"
    if (
        not isinstance(document, dict)
        or set(document) != required
        or document.get("snapshot_index_version") != 1
        or raw != canonical
        or not isinstance(roots, list)
        or not isinstance(manifests, dict)
        or not isinstance(files, list)
        or not isinstance(links, list)
    ):
        raise ValidationError("runtime snapshot index is malformed")
    if any(not isinstance(root, str) for root in roots) or roots != sorted(set(roots)):
        raise ValidationError("runtime snapshot index roots are malformed")
    if set(manifests) != set(roots) or any(
        not _valid_sha256(value) for value in manifests.values()
    ):
        raise ValidationError("runtime snapshot index manifests are malformed")
    missing: list[str] = []
    mismatched: list[str] = []
    type_mismatches: list[str] = []
    hash_mismatches: list[str] = []
    temps: list[str] = []
    orphans: list[str] = []
    for root in roots:
        manifest_path = f"{root}/{SNAPSHOT_MANIFEST}"
        try:
            actual_manifest = content_hash(
                _read_metadata(Path(home) / root, SNAPSHOT_MANIFEST)
            )
        except FileNotFoundError:
            missing.append(manifest_path)
        except (OSError, ValidationError):
            type_mismatches.append(manifest_path)
        else:
            if actual_manifest != manifests[root]:
                hash_mismatches.append(manifest_path)
        try:
            inspection = inspect_managed_snapshot(home, root)
        except ValidationError:
            type_mismatches.append(root)
            continue
        missing.extend(f"{root}/{path}" for path in inspection.referenced_missing)
        type_mismatches.extend(
            f"{root}/{path}" for path in inspection.type_mismatches
        )
        hash_mismatches.extend(
            f"{root}/{path}" for path in inspection.hash_mismatches
        )
        temps.extend(f"{root}/{path}" for path in inspection.owned_temps)
        orphans.extend(f"{root}/{path}" for path in inspection.orphans)
    expected_files = _entry_map(files)
    for path, expected in expected_files.items():
        try:
            entries, _ = _read_tree(home, (PurePosixPath(path),))
            actual = next(entry for entry in entries if entry["path"] == path)
        except ValidationError:
            try:
                entry_type = managed_entry_type(home, path)
            except ValidationError:
                type_mismatches.append(path)
            else:
                if entry_type == "missing":
                    missing.append(path)
                elif entry_type == "file":
                    hash_mismatches.append(path)
                else:
                    type_mismatches.append(path)
        except StopIteration:
            missing.append(path)
        else:
            if actual != expected:
                if actual.get("type") != expected.get("type"):
                    type_mismatches.append(path)
                else:
                    hash_mismatches.append(path)
    expected_links: dict[str, str] = {}
    for entry in links:
        if (
            not isinstance(entry, dict)
            or set(entry) != {"path", "target"}
            or not isinstance(entry.get("path"), str)
            or not isinstance(entry.get("target"), str)
            or entry["path"] in expected_links
        ):
            raise ValidationError("runtime snapshot index links are malformed")
        expected_links[entry["path"]] = entry["target"]
    if list(expected_links) != sorted(expected_links):
        raise ValidationError("runtime snapshot index links are malformed")
    for path, target in expected_links.items():
        try:
            actual_target = read_managed_symlink(home, path)
        except ValidationError:
            type_mismatches.append(path)
        else:
            if actual_target is None:
                missing.append(path)
            elif actual_target != target:
                hash_mismatches.append(path)
    mismatched.extend(type_mismatches)
    mismatched.extend(hash_mismatches)
    return RuntimeSnapshotInspection(
        not (missing or mismatched or temps or orphans),
        tuple(sorted(set(missing))),
        tuple(sorted(set(mismatched))),
        tuple(sorted(set(temps))),
        tuple(sorted(set(orphans))),
        tuple(sorted(set(type_mismatches))),
        tuple(sorted(set(hash_mismatches))),
    )
