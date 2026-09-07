"""Safe writes beneath an adapter's generated home."""

from __future__ import annotations

import errno
import hashlib
import os
import secrets
import tempfile
from pathlib import Path

from ..errors import PathEscapeError, ValidationError
from ..verify import (
    ANSWER_FORMAT_CONTENT,
    ANSWER_FORMAT_FILENAME,
    ANSWER_PROOF_SUFFIX,
    answer_proof_document,
)

MANAGED_TEMP_PREFIX = ".agent-run-tmp-"
"""Reserved prefix identifying temporary files owned by managed publication."""


def content_hash(content: str | bytes) -> str:
    data = content.encode("utf-8") if isinstance(content, str) else content
    if not isinstance(data, bytes):
        raise ValidationError("managed content must be str or bytes")
    return hashlib.sha256(data).hexdigest()


def managed_uv_python_environment() -> dict[str, str]:
    """Return uv's existing managed-install root for isolated child processes.

    uv normally derives its install directory from ``HOME``. Agent-run replaces
    that value for children, so this exposes only an already-present parent
    installation through uv's documented ``UV_PYTHON_INSTALL_DIR`` override.
    An explicit override wins; otherwise XDG data location and then uv's
    default ``~/.local/share`` location are checked. Missing or non-directory
    paths yield no variable, preserving uv's normal failure behavior.
    """

    configured = os.environ.get("UV_PYTHON_INSTALL_DIR")
    data_home = os.environ.get("XDG_DATA_HOME")
    candidate = (
        Path(configured).expanduser()
        if configured
        else (Path(data_home).expanduser() if data_home else Path.home() / ".local" / "share")
        / "uv"
        / "python"
    )
    try:
        root = candidate.resolve(strict=True)
    except OSError:
        return {}
    return {"UV_PYTHON_INSTALL_DIR": str(root)} if root.is_dir() else {}


def _root(home: str | Path) -> Path:
    try:
        path = Path(home).expanduser()
    except (TypeError, RuntimeError) as error:
        raise ValidationError("generated home must be an absolute path") from error
    if not path.is_absolute():
        raise ValidationError("generated home must be an absolute path")
    if path.is_symlink():
        raise PathEscapeError(f"generated home must not be a symlink: {path}")
    try:
        path.mkdir(mode=0o700, parents=True, exist_ok=True)
        root = path.resolve(strict=True)
    except OSError as error:
        raise ValidationError(f"cannot create generated home {path}: {error}") from error
    if not root.is_dir():
        raise ValidationError(f"generated home is not a directory: {root}")
    return root


def _managed_path(home: str | Path, relative_path: str | Path) -> Path:
    root = _root(home)
    try:
        relative = Path(relative_path)
    except TypeError as error:
        raise PathEscapeError(f"invalid managed path: {relative_path!r}") from error
    if relative.is_absolute() or not relative.parts or ".." in relative.parts:
        raise PathEscapeError(f"managed path escapes generated home: {relative_path}")
    parent = root
    for part in relative.parts[:-1]:
        parent /= part
        if parent.is_symlink():
            raise PathEscapeError(f"managed path crosses a symlink: {relative_path}")
        try:
            parent.mkdir(mode=0o700)
            _fsync_directory(parent.parent)
        except FileExistsError:
            if not parent.is_dir():
                raise ValidationError(f"managed path parent is not a directory: {relative_path}")
        except OSError as error:
            raise ValidationError(f"cannot create managed directory {parent}: {error}") from error
        if not parent.resolve(strict=True).is_relative_to(root):
            raise PathEscapeError(f"managed path escapes generated home: {relative_path}")
    candidate = parent / relative.name
    if not candidate.parent.resolve(strict=True).is_relative_to(root):
        raise PathEscapeError(f"managed path escapes generated home: {relative_path}")
    return candidate


def _fsync_directory(path: Path) -> None:
    """Persist prior directory-entry changes beneath an existing directory.

    ``path`` is opened read-only as a directory and synchronized before the
    descriptor is closed. Filesystems that report directory fsync as unsupported
    are tolerated; other ``OSError`` failures propagate so callers cannot claim
    an ordered durable publish when an available sync operation failed.
    """

    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        try:
            os.fsync(descriptor)
        except OSError as error:
            if error.errno not in {errno.EINVAL, errno.ENOTSUP}:
                raise
    finally:
        os.close(descriptor)


def ensure_managed_directory(home: str | Path, relative_path: str | Path) -> Path:
    """Create and return one validated private directory below ``home``.

    The nonempty ``relative_path`` must remain beneath the generated home.
    Newly created parent entries are synchronized and symlink crossings or
    non-directory conflicts raise typed validation errors.
    """

    try:
        relative = Path(relative_path)
    except TypeError as error:
        raise PathEscapeError(f"invalid managed directory: {relative_path!r}") from error
    if not relative.parts or relative == Path("."):
        raise PathEscapeError("managed directory path must be nonempty")
    return _managed_path(home, relative / ".agent-run-directory").parent


def write_managed_file(
    home: str | Path,
    relative_path: str | Path,
    content: str | bytes,
    *,
    mode: int = 0o600,
) -> str:
    """Durably replace one private regular file and return its SHA-256.

    ``home`` owns the generated tree, ``relative_path`` must stay beneath it,
    and ``content`` supplies the exact UTF-8 or byte payload. ``mode`` is either
    private data ``0600`` or private executable ``0700``. The temporary file
    is synchronized before its atomic replacement, then the parent directory is
    synchronized so a successful return makes that one publish durable.
    Validation and path-escape errors are typed; filesystem failures propagate.
    """

    data = content.encode("utf-8") if isinstance(content, str) else content
    if mode not in {0o600, 0o700}:
        raise ValidationError("managed file mode must be 0600 or 0700")
    digest = content_hash(data)
    candidate = _managed_path(home, relative_path)
    if candidate.is_symlink():
        raise PathEscapeError(f"managed file must not replace a symlink: {relative_path}")
    if candidate.exists() and not candidate.is_file():
        raise ValidationError(f"managed path is not a file: {relative_path}")
    descriptor, temporary_name = tempfile.mkstemp(
        dir=candidate.parent,
        prefix=f"{MANAGED_TEMP_PREFIX}{candidate.name}.",
        suffix=".tmp",
    )
    temporary = Path(temporary_name)
    try:
        os.fchmod(descriptor, mode)
        with os.fdopen(descriptor, "wb") as stream:
            descriptor = -1
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, candidate)
        _fsync_directory(candidate.parent)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        temporary.unlink(missing_ok=True)
    return digest


def seal_answer(path: Path, text: str) -> tuple[int, str]:
    """Seal one completed engine answer as exact payload bytes plus a proof.

    The payload file holds ``text`` encoded as UTF-8 with no appended
    completion marker. The adjacent ``<name>.proof.json`` sidecar records the
    payload's byte count and SHA-256. A directory-level format marker is
    durably written first so a crash or deleted proof cannot make a new payload
    look like a historical sentinel-framed answer. The marker, payload, and
    proof are separate ordered durable replacements, not one cross-file atomic
    transaction.

    Returns ``(payload_bytes, payload_sha256)`` for the clean payload, the
    values durably recorded with the run's outcome.
    """

    if not isinstance(path, Path) or not path.is_absolute():
        raise ValidationError("answer path must be absolute")
    if not isinstance(text, str) or not text.strip():
        raise ValidationError("answer text must be nonblank")
    data = text.encode("utf-8")
    write_managed_file(path.parent, ANSWER_FORMAT_FILENAME, ANSWER_FORMAT_CONTENT)
    digest = write_managed_file(path.parent, path.name, data)
    write_managed_file(
        path.parent,
        f"{path.name}{ANSWER_PROOF_SUFFIX}",
        answer_proof_document(path.name, len(data), digest),
    )
    return len(data), digest


def create_symlink_bridge(
    home: str | Path, relative_path: str | Path, source: str | Path
) -> Path:
    """Atomically create an explicit bridge at a validated location."""

    try:
        source_path = Path(source).expanduser()
    except (TypeError, RuntimeError) as error:
        raise ValidationError("symlink bridge source must be an absolute existing path") from error
    if not source_path.is_absolute():
        raise ValidationError("symlink bridge source must be an absolute existing path")
    try:
        source_path = source_path.resolve(strict=True)
    except (OSError, RuntimeError) as error:
        raise ValidationError("symlink bridge source must be an absolute existing path") from error
    candidate = _managed_path(home, relative_path)
    if candidate.exists() and not candidate.is_symlink():
        raise ValidationError(f"symlink bridge would replace a managed file: {relative_path}")
    temporary = candidate.parent / f".{candidate.name}.{secrets.token_hex(8)}.link.tmp"
    try:
        temporary.symlink_to(source_path, target_is_directory=source_path.is_dir())
        os.replace(temporary, candidate)
    finally:
        temporary.unlink(missing_ok=True)
    return candidate
