"""Immutable managed-tree publication and recovery inspection."""

from __future__ import annotations

import hashlib
import json
import os
import stat
from dataclasses import dataclass
from pathlib import Path, PurePosixPath

from ..config import EnvironmentConfig, RuntimeConfig
from ..errors import PathEscapeError, ValidationError
from ..profiles import AgentProfile
from .home import (
    MANAGED_TEMP_PREFIX,
    content_hash,
    ensure_managed_directory,
    write_managed_file,
)

SNAPSHOT_MANIFEST = ".agent-run-snapshot.json"
"""Metadata filename published after a managed snapshot's content."""

CONFIG_SNAPSHOT_FILENAME = "config-snapshot.json"
"""Attempt-relative filename for effective configuration evidence."""

RUNTIME_SNAPSHOT_INDEX = ".agent-run-snapshots.json"
"""Generated-home index of every managed snapshot root required on resume."""

_MAX_SNAPSHOT_METADATA_BYTES = 64 * 1024
"""Independent read bound for runtime/config snapshot metadata."""


@dataclass(frozen=True, slots=True)
class TreeSnapshot:
    """Published tree identity: manifest hash, path, and canonical entry paths."""

    sha256: str
    manifest_path: Path
    entries: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class SnapshotInspection:
    """Non-destructive recovery classification for one managed snapshot.

    Verification requires exact agreement with the manifest. Reserved publisher
    temporaries, unreferenced orphans, missing references, and type/content
    mismatches remain separate and inspection never removes them.
    """

    verified: bool
    owned_temps: tuple[str, ...]
    orphans: tuple[str, ...]
    referenced_missing: tuple[str, ...]
    mismatched: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class ConfigSnapshot:
    """Configuration bytes, their SHA-256, and the bound runtime-index hash."""

    document: bytes
    sha256: str
    snapshot_index_sha256: str
    materialize_revision: str
    runtime_version: str | None


@dataclass(frozen=True, slots=True)
class RuntimeSnapshotInspection:
    """Aggregate recovery result for every indexed managed runtime artifact."""

    verified: bool
    missing: tuple[str, ...]
    mismatched: tuple[str, ...]
    owned_temps: tuple[str, ...]
    orphans: tuple[str, ...]


def _relative(value: str | Path, label: str) -> Path:
    """Return a nonempty managed relative path or raise ``PathEscapeError``."""

    try:
        path = Path(value)
    except TypeError as error:
        raise PathEscapeError(f"{label} must be a relative path") from error
    if path.is_absolute() or not path.parts or ".." in path.parts or path == Path("."):
        raise PathEscapeError(f"{label} must be a relative path without '..'")
    return path


def _read_tree(
    source: Path, selected: tuple[PurePosixPath, ...] | None = None
) -> tuple[list[dict[str, object]], dict[str, bytes]]:
    """Read path/type/content evidence through no-follow descriptors.

    ``source`` must be an absolute real directory. ``selected`` optionally names
    the exact relative files/directories to traverse; their parent directories
    are retained for a self-contained layout. Selected descendants may only be
    real directories or regular files, so links and special files fail closed.
    """

    if not isinstance(source, Path) or not source.is_absolute():
        raise ValidationError("snapshot source must be an absolute directory")
    directory_flags = os.O_RDONLY | os.O_CLOEXEC | os.O_DIRECTORY | os.O_NOFOLLOW
    try:
        root = os.open(source, directory_flags)
    except OSError as error:
        raise ValidationError(f"snapshot source must be a real directory: {source}") from error
    entries: list[dict[str, object]] = []
    files: dict[str, bytes] = {}

    def walk(descriptor: int, prefix: PurePosixPath) -> None:
        """Read one opened directory and recurse only through opened children."""

        try:
            names = sorted(os.listdir(descriptor))
        except OSError as error:
            raise ValidationError(f"cannot list snapshot source: {source}") from error
        for name in names:
            relative = prefix / name
            portable = relative.as_posix()
            included = selected is None or any(
                relative == item or item in relative.parents for item in selected
            )
            traversed = included or (
                selected is not None and any(relative in item.parents for item in selected)
            )
            if not traversed:
                continue
            try:
                metadata = os.stat(name, dir_fd=descriptor, follow_symlinks=False)
            except OSError as error:
                raise ValidationError(f"cannot inspect snapshot entry: {portable}") from error
            if stat.S_ISDIR(metadata.st_mode):
                try:
                    child = os.open(name, directory_flags, dir_fd=descriptor)
                except OSError as error:
                    raise ValidationError(f"snapshot directory must not be a symlink: {portable}") from error
                entries.append({"path": portable, "type": "directory"})
                try:
                    walk(child, relative)
                finally:
                    os.close(child)
                continue
            if not stat.S_ISREG(metadata.st_mode):
                raise ValidationError(f"snapshot entry must be regular: {portable}")
            try:
                opened = os.open(
                    name, os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW, dir_fd=descriptor
                )
                try:
                    current = os.fstat(opened)
                    if not stat.S_ISREG(current.st_mode):
                        raise ValidationError(f"snapshot entry must be regular: {portable}")
                    with os.fdopen(opened, "rb") as stream:
                        payload = stream.read()
                except BaseException:
                    try:
                        os.close(opened)
                    except OSError:
                        pass
                    raise
            except OSError as error:
                raise ValidationError(f"cannot read snapshot file: {portable}") from error
            files[portable] = payload
            mode = 0o700 if current.st_mode & 0o111 else 0o600
            entries.append(
                {
                    "path": portable,
                    "type": "file",
                    "mode": mode,
                    "bytes": len(payload),
                    "sha256": hashlib.sha256(payload).hexdigest(),
                }
            )

    try:
        walk(root, PurePosixPath())
    finally:
        os.close(root)
    entries.sort(key=lambda entry: str(entry["path"]))
    if selected is not None:
        found = {PurePosixPath(str(entry["path"])) for entry in entries}
        missing = sorted(item.as_posix() for item in selected if item not in found)
        if missing:
            raise ValidationError(f"snapshot assets are missing: {', '.join(missing)}")
    return entries, files


def _manifest(entries: list[dict[str, object]]) -> bytes:
    """Serialize version-one entries into canonical UTF-8 JSON bytes."""

    return (
        json.dumps(
            {"snapshot_version": 1, "entries": entries},
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        + b"\n"
    )


def _load_manifest(path: Path) -> list[dict[str, object]] | None:
    """Load one no-follow manifest, or return ``None`` when it is absent."""

    try:
        descriptor = os.open(path, os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW)
    except FileNotFoundError:
        return None
    except OSError as error:
        raise ValidationError("snapshot manifest is unreadable") from error
    try:
        with os.fdopen(descriptor, "rb") as stream:
            metadata = os.fstat(stream.fileno())
            if not stat.S_ISREG(metadata.st_mode):
                raise ValidationError("snapshot manifest must be a regular file")
            raw = stream.read()
    except OSError as error:
        raise ValidationError("snapshot manifest is unreadable") from error
    try:
        document = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValidationError("snapshot manifest is malformed") from error
    if not isinstance(document, dict) or document.get("snapshot_version") != 1:
        raise ValidationError("snapshot manifest version is unsupported")
    entries = document.get("entries")
    if not isinstance(entries, list) or any(not isinstance(entry, dict) for entry in entries):
        raise ValidationError("snapshot manifest entries are malformed")
    return entries


def _read_metadata(directory: Path, name: str) -> bytes:
    """Read one no-follow regular metadata file relative to an owned directory."""

    directory_fd = os.open(
        directory, os.O_RDONLY | os.O_CLOEXEC | os.O_DIRECTORY | os.O_NOFOLLOW
    )
    try:
        descriptor = os.open(
            name, os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW, dir_fd=directory_fd
        )
    finally:
        os.close(directory_fd)
    with os.fdopen(descriptor, "rb") as stream:
        metadata = os.fstat(stream.fileno())
        if not stat.S_ISREG(metadata.st_mode):
            raise ValidationError(f"{name} must be a regular file")
        if metadata.st_size > _MAX_SNAPSHOT_METADATA_BYTES:
            raise ValidationError(f"{name} exceeds the snapshot metadata bound")
        payload = stream.read(_MAX_SNAPSHOT_METADATA_BYTES + 1)
        if len(payload) > _MAX_SNAPSHOT_METADATA_BYTES:
            raise ValidationError(f"{name} exceeds the snapshot metadata bound")
        return payload


def _register_snapshot(home: Path, relative: Path) -> None:
    """Durably add one canonical managed root to the runtime snapshot index."""

    try:
        raw = _read_metadata(home, RUNTIME_SNAPSHOT_INDEX)
    except FileNotFoundError:
        roots: list[str] = []
    else:
        try:
            document = json.loads(raw)
            roots = document["roots"]
        except (UnicodeDecodeError, json.JSONDecodeError, KeyError, TypeError) as error:
            raise ValidationError("runtime snapshot index is malformed") from error
        if document.get("snapshot_index_version") != 1 or not isinstance(roots, list):
            raise ValidationError("runtime snapshot index is malformed")
    root = relative.as_posix()
    if root not in roots:
        roots.append(root)
    payload = (
        json.dumps(
            {"snapshot_index_version": 1, "roots": sorted(roots)},
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        + b"\n"
    )
    write_managed_file(home, RUNTIME_SNAPSHOT_INDEX, payload)


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


def _entry_map(entries: list[dict[str, object]]) -> dict[str, dict[str, object]]:
    """Validate manifest entries and return their unique path mapping."""

    mapped: dict[str, dict[str, object]] = {}
    for entry in entries:
        path = entry.get("path")
        kind = entry.get("type")
        if not isinstance(path, str) or not path or kind not in {"directory", "file"}:
            raise ValidationError("snapshot manifest entries are malformed")
        relative = PurePosixPath(path)
        if relative.is_absolute() or ".." in relative.parts or path == SNAPSHOT_MANIFEST:
            raise ValidationError("snapshot manifest path is invalid")
        if path in mapped:
            raise ValidationError("snapshot manifest path is duplicated")
        mapped[path] = entry
    return mapped


def inspect_managed_snapshot(home: Path, relative_root: str | Path) -> SnapshotInspection:
    """Classify snapshot recovery state without deleting or repairing paths."""

    root = Path(home) / _relative(relative_root, "snapshot destination")
    if not root.exists():
        return SnapshotInspection(False, (), (), (SNAPSHOT_MANIFEST,), ())
    if root.is_symlink() or not root.is_dir():
        raise ValidationError("snapshot destination must be a real directory")
    expected_entries = _load_manifest(root / SNAPSHOT_MANIFEST)
    actual_entries, _ = _read_tree(root)
    actual: dict[str, dict[str, object]] = {}
    owned_temps: list[str] = []
    for entry in actual_entries:
        path = str(entry["path"])
        name = PurePosixPath(path).name
        if path == SNAPSHOT_MANIFEST:
            continue
        if name.startswith(MANAGED_TEMP_PREFIX) and name.endswith(".tmp"):
            owned_temps.append(path)
        else:
            actual[path] = entry
    if expected_entries is None:
        return SnapshotInspection(
            False, tuple(sorted(owned_temps)), tuple(sorted(actual)), (SNAPSHOT_MANIFEST,), ()
        )
    expected = _entry_map(expected_entries)
    missing = tuple(sorted(set(expected) - set(actual)))
    orphans = tuple(sorted(set(actual) - set(expected)))
    mismatched = tuple(
        path for path in sorted(set(expected) & set(actual)) if expected[path] != actual[path]
    )
    temps = tuple(sorted(owned_temps))
    return SnapshotInspection(
        not (temps or missing or orphans or mismatched), temps, orphans, missing, mismatched
    )


def _publish_snapshot(
    home: Path,
    relative: Path,
    entries: list[dict[str, object]],
    files: dict[str, bytes],
) -> TreeSnapshot:
    """Publish prepared snapshot entries and canonical metadata without deletion."""

    if any(str(entry["path"]) == SNAPSHOT_MANIFEST for entry in entries):
        raise ValidationError(f"snapshot source uses reserved name: {SNAPSHOT_MANIFEST}")
    root = ensure_managed_directory(home, relative)
    if any(root.iterdir()):
        inspection = inspect_managed_snapshot(Path(home), relative)
        previous = _load_manifest(root / SNAPSHOT_MANIFEST)
        if not inspection.verified or previous is None:
            raise ValidationError("existing managed snapshot requires recovery")
        old = _entry_map(previous)
        new = _entry_map(entries)
        if old.keys() != new.keys() or any(old[path]["type"] != new[path]["type"] for path in new):
            raise ValidationError("snapshot publication cannot change existing topology")
    for entry in entries:
        if entry["type"] == "directory":
            ensure_managed_directory(home, relative / str(entry["path"]))
    by_path = _entry_map(entries)
    for path, payload in sorted(files.items()):
        write_managed_file(
            home, relative / path, payload, mode=int(by_path[path]["mode"])
        )
    document = _manifest(entries)
    write_managed_file(home, relative / SNAPSHOT_MANIFEST, document)
    _register_snapshot(Path(home), relative)
    return TreeSnapshot(
        content_hash(document),
        root / SNAPSHOT_MANIFEST,
        tuple(str(entry["path"]) for entry in entries),
    )


def snapshot_managed_tree(
    home: Path, relative_root: str | Path, source: Path
) -> TreeSnapshot:
    """Publish a full tree and its canonical manifest last without deletion.

    A destination must be empty or a verified snapshot with identical topology;
    publication never deletes content. Source symlinks and special files are
    rejected. Every source directory and regular file is durably published before
    final metadata, so interruption remains unverified and recoverable.
    """

    return _publish_snapshot(
        home,
        _relative(relative_root, "snapshot destination"),
        *_read_tree(source),
    )


def snapshot_selected_assets(
    home: Path,
    relative_root: str | Path,
    source: Path,
    assets: tuple[str, ...],
) -> TreeSnapshot:
    """Publish only explicitly declared non-secret plugin assets.

    ``assets`` contains unique relative POSIX files or directories beneath the
    configured ``source`` plugin root. No globs, discovery, import tracing, or
    filename denylist is applied: the trusted declaration owns completeness and
    secrecy. Containment, presence, no-follow types, and bytes are validated by
    the same descriptor and manifest path as full-tree snapshots.
    """

    selected: list[PurePosixPath] = []
    for value in assets:
        if not isinstance(value, str) or not value or "\0" in value:
            raise ValidationError("plugin snapshot assets must be nonempty relative paths")
        path = PurePosixPath(value)
        if path.is_absolute() or "." in path.parts or ".." in path.parts:
            raise PathEscapeError(f"plugin snapshot asset escapes its root: {value}")
        if any(character in value for character in "*?[]{}"):
            raise ValidationError(f"plugin snapshot asset must not use glob syntax: {value}")
        if path in selected:
            raise ValidationError(f"plugin snapshot asset is duplicated: {value}")
        selected.append(path)
    if not selected:
        raise ValidationError("plugin snapshot assets must not be empty")
    return _publish_snapshot(
        home,
        _relative(relative_root, "snapshot destination"),
        *_read_tree(source, tuple(selected)),
    )


def _environment_document(environment: EnvironmentConfig | None) -> object:
    """Return deterministic environment evidence without raw variable values."""

    if environment is None:
        return None
    return {
        "path": [str(path) for path in environment.path],
        "variable_sha256": {
            name: content_hash(value) for name, value in sorted(environment.variables.items())
        },
        "required_commands": list(environment.required_commands),
        "denied_commands": list(environment.denied_commands),
        "rust": None
        if environment.rust is None
        else [str(environment.rust.rustup_home), str(environment.rust.cargo_bin)],
    }


def _runtime_document(config: RuntimeConfig) -> dict[str, object]:
    """Return complete deterministic runtime declarations without credential bytes."""

    auth = None
    if config.auth is not None:
        auth = {
            "kind": config.auth.kind,
            "source": None if config.auth.source is None else str(config.auth.source),
            "target": config.auth.target,
            "names": list(config.auth.names),
        }
    return {
        "enabled": config.enabled,
        "adapter": config.adapter,
        "binary": str(config.binary),
        "home": str(config.home),
        "models": list(config.models),
        "skills": list(config.skills),
        "mcp": list(config.mcp),
        "max_active_agents": config.max_active_agents,
        "auth": auth,
        "hooks": [[hook.event, list(hook.command), hook.matcher] for hook in config.hooks],
        "plugins": [str(path) for path in config.plugins],
        "plugin_snapshot_assets": {
            name: list(paths)
            for name, paths in sorted(
                getattr(config, "plugin_snapshot_assets", {}).items()
            )
        },
        "limits_source": config.limits_source,
        "accounts": list(config.accounts),
        "default_account": config.default_account,
        "priority_multiplier": config.priority_multiplier,
        "priority_account_multipliers": dict(sorted(config.priority_account_multipliers.items())),
        "priority_lane_multipliers": dict(sorted(config.priority_lane_multipliers.items())),
        "rust": None
        if config.rust is None
        else [str(config.rust.rustup_home), str(config.rust.cargo_bin)],
        "environment": _environment_document(config.environment),
    }


def _profile_document(profile: AgentProfile) -> dict[str, object]:
    """Return the effective profile bytes and every current grant field."""

    return {
        name: (
            [str(item) for item in value]
            if isinstance(value, tuple) and all(isinstance(item, Path) for item in value)
            else value
        )
        for name, value in vars(profile).items()
    }


def build_config_snapshot(
    *,
    runtime: str,
    adapter_api_version: int,
    schema_version: int,
    materialize_revision: str,
    snapshot_index_sha256: str,
    config: RuntimeConfig,
    profile: AgentProfile,
    runtime_version: str | None = None,
) -> ConfigSnapshot:
    """Build canonical effective configuration evidence for one attempt.

    The document binds runtime, adapter/config versions, all runtime declarations
    through a credential-free hash, the materialized-file revision, and the
    effective profile body and grants. ``runtime_version`` records already-known
    native version evidence or remains ``None`` without triggering a probe.
    Environment values are hashed rather than stored. Identical inputs yield
    identical bytes; content-only changes alter the returned SHA-256.
    """

    if not isinstance(runtime, str) or not runtime.strip():
        raise ValidationError("snapshot runtime must be nonblank")
    if type(adapter_api_version) is not int or adapter_api_version < 1:
        raise ValidationError("snapshot adapter_api_version must be positive")
    if type(schema_version) is not int or schema_version < 1:
        raise ValidationError("snapshot schema_version must be positive")
    if not isinstance(materialize_revision, str) or not materialize_revision.strip():
        raise ValidationError("snapshot materialize_revision must be nonblank")
    if not _valid_sha256(snapshot_index_sha256):
        raise ValidationError("snapshot index sha256 must be 64 hexadecimal characters")
    if not isinstance(config, RuntimeConfig) or not isinstance(profile, AgentProfile):
        raise ValidationError("snapshot requires RuntimeConfig and AgentProfile")
    if runtime_version is not None and (
        not isinstance(runtime_version, str) or not runtime_version.strip()
    ):
        raise ValidationError("snapshot runtime_version must be nonblank or None")
    runtime_bytes = json.dumps(
        _runtime_document(config), sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    document = (
        json.dumps(
            {
                "snapshot_version": 1,
                "runtime": runtime,
                "runtime_version": runtime_version,
                "adapter_api_version": adapter_api_version,
                "config_schema_version": schema_version,
                "runtime_config_sha256": content_hash(runtime_bytes),
                "materialize_revision": materialize_revision,
                "snapshot_index_sha256": snapshot_index_sha256,
                "profile": _profile_document(profile),
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        + b"\n"
    )
    if len(document) > _MAX_SNAPSHOT_METADATA_BYTES:
        raise ValidationError("config snapshot exceeds the metadata bound")
    return ConfigSnapshot(
        document,
        content_hash(document),
        snapshot_index_sha256,
        materialize_revision,
        runtime_version,
    )


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


def inspect_config_snapshot(candidate_dir: Path, expected_sha256: str) -> ConfigSnapshot:
    """Read, hash, and structurally validate persisted configuration evidence.

    ``candidate_dir`` is the owned attempt directory and ``expected_sha256`` is
    its durable recorded revision. The file must be canonical version-one JSON,
    regular, no-follow, and byte-identical to its hash; malformed, missing, or
    contradictory evidence raises ``ValidationError``.
    """

    if not _valid_sha256(expected_sha256):
        raise ValidationError("config snapshot sha256 must be 64 hexadecimal characters")
    try:
        raw = _read_metadata(candidate_dir, CONFIG_SNAPSHOT_FILENAME)
    except FileNotFoundError as error:
        raise ValidationError("config snapshot is missing") from error
    if content_hash(raw) != expected_sha256:
        raise ValidationError("config snapshot hash does not match its recorded revision")
    try:
        document = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValidationError("config snapshot is malformed") from error
    required = {
        "snapshot_version",
        "runtime",
        "runtime_version",
        "adapter_api_version",
        "config_schema_version",
        "runtime_config_sha256",
        "materialize_revision",
        "snapshot_index_sha256",
        "profile",
    }
    canonical = json.dumps(document, sort_keys=True, separators=(",", ":")).encode("utf-8") + b"\n"
    if not isinstance(document, dict) or set(document) != required or document.get("snapshot_version") != 1:
        raise ValidationError("config snapshot shape is unsupported")
    if raw != canonical:
        raise ValidationError("config snapshot is not canonical")
    index_sha256 = document["snapshot_index_sha256"]
    if not _valid_sha256(index_sha256):
        raise ValidationError("config snapshot index sha256 is invalid")
    materialize_revision = document["materialize_revision"]
    runtime_version = document["runtime_version"]
    if not isinstance(materialize_revision, str) or not materialize_revision.strip():
        raise ValidationError("config snapshot materialize revision is invalid")
    if runtime_version is not None and (
        not isinstance(runtime_version, str) or not runtime_version.strip()
    ):
        raise ValidationError("config snapshot runtime version is invalid")
    return ConfigSnapshot(
        raw,
        expected_sha256,
        index_sha256,
        materialize_revision,
        runtime_version,
    )
