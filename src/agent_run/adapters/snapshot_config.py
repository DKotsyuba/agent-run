"""Canonical effective-configuration snapshot creation and inspection."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

from ..config import EnvironmentConfig, RuntimeConfig
from ..errors import ValidationError
from ..profiles import AgentProfile
from .home import content_hash
from .snapshot_runtime import _valid_sha256
from .snapshot_tree import _MAX_SNAPSHOT_METADATA_BYTES, _read_metadata

CONFIG_SNAPSHOT_FILENAME = "config-snapshot.json"
"""Attempt-relative filename for effective configuration evidence."""


@dataclass(frozen=True, slots=True)
class ConfigSnapshot:
    """Configuration bytes, their SHA-256, and the bound runtime-index hash."""

    document: bytes
    sha256: str
    snapshot_index_sha256: str
    materialize_revision: str
    runtime_version: str | None

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
        "credential_state_home": (
            None
            if config.credential_state_home is None
            else str(config.credential_state_home)
        ),
        "models": list(config.models),
        "skills": list(config.skills),
        "mcp": list(config.mcp),
        "max_active_agents": config.max_active_agents,
        "auth": auth,
        "hooks": [[hook.event, list(hook.command), hook.matcher] for hook in config.hooks],
        "plugins": [str(path) for path in config.plugins],
        "plugin_snapshot_assets": {
            name: list(paths)
            for name, paths in sorted(config.plugin_snapshot_assets.items())
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
    runtime_document = _runtime_document(config)
    runtime_bytes = json.dumps(
        runtime_document, sort_keys=True, separators=(",", ":")
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
                "runtime_config": runtime_document,
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
        "runtime_config",
        "materialize_revision",
        "snapshot_index_sha256",
        "profile",
    }
    canonical = json.dumps(document, sort_keys=True, separators=(",", ":")).encode("utf-8") + b"\n"
    if not isinstance(document, dict) or set(document) != required or document.get("snapshot_version") != 1:
        raise ValidationError("config snapshot shape is unsupported")
    if raw != canonical:
        raise ValidationError("config snapshot is not canonical")
    runtime_document = document["runtime_config"]
    runtime_bytes = json.dumps(
        runtime_document, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    if content_hash(runtime_bytes) != document["runtime_config_sha256"]:
        raise ValidationError("config snapshot runtime declaration hash is invalid")
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
