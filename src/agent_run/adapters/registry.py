"""Trusted local runtime adapter loading."""

from __future__ import annotations

import importlib
import re
from collections.abc import Iterable, Mapping

from ..config import Config, RuntimeConfig
from ..errors import ValidationError
from .base import ADAPTER_API_VERSION, Capability, RuntimeAdapter, RuntimeInfo


_IMPORT_REF = re.compile(
    r"[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*:[A-Za-z_][A-Za-z0-9_]*\Z"
)
_REQUIRED_METHODS = (
    "describe",
    "validate",
    "materialize",
    "probe",
    "models",
    "limits",
    "prepare",
    "launch",
)


def load_adapter(
    config_or_reference: RuntimeConfig | str,
    required_capabilities: Iterable[Capability] = (),
) -> RuntimeAdapter:
    """Import and validate an adapter without invoking a runtime."""

    reference = (
        config_or_reference.adapter
        if isinstance(config_or_reference, RuntimeConfig)
        else config_or_reference
    )
    if not isinstance(reference, str) or not _IMPORT_REF.fullmatch(reference):
        raise ValidationError("adapter must be a trusted local 'module:attribute' reference")
    module_name, attribute = reference.split(":", 1)
    try:
        module = importlib.import_module(module_name)
    except (ImportError, ValueError) as error:
        raise ValidationError(f"cannot import adapter module {module_name}: {error}") from error
    version = getattr(module, "ADAPTER_API_VERSION", None)
    if type(version) is not int or version != ADAPTER_API_VERSION:
        raise ValidationError(
            f"adapter {reference} API version {version!r} does not match {ADAPTER_API_VERSION}"
        )
    try:
        adapter = getattr(module, attribute)
    except AttributeError as error:
        raise ValidationError(f"adapter attribute does not exist: {reference}") from error
    for method in _REQUIRED_METHODS:
        if not callable(getattr(adapter, method, None)):
            raise ValidationError(f"adapter {reference} is missing callable {method}")
    try:
        info = adapter.describe()
    except Exception as error:
        raise ValidationError(f"adapter {reference} describe failed: {error}") from error
    if not isinstance(info, RuntimeInfo):
        raise ValidationError(f"adapter {reference} describe must return RuntimeInfo")
    if type(info.adapter_api_version) is not int or info.adapter_api_version != ADAPTER_API_VERSION:
        raise ValidationError(
            f"adapter {reference} reports API version {info.adapter_api_version!r}, expected {ADAPTER_API_VERSION}"
        )
    if not isinstance(info.capabilities, frozenset) or any(
        not isinstance(capability, Capability) for capability in info.capabilities
    ):
        raise ValidationError(f"adapter {reference} reports invalid capabilities")
    required = frozenset(required_capabilities)
    if any(not isinstance(capability, Capability) for capability in required):
        raise ValidationError("required capabilities must be Capability values")
    missing = required - info.capabilities
    if missing:
        names = ", ".join(sorted(capability.value for capability in missing))
        raise ValidationError(f"adapter {reference} lacks required capabilities: {names}")
    return adapter


class AdapterRegistry:
    def __init__(self, config: Config | Mapping[str, RuntimeConfig]) -> None:
        self._runtimes = config.runtimes if isinstance(config, Config) else config

    def load(
        self, name: str, required_capabilities: Iterable[Capability] = ()
    ) -> RuntimeAdapter:
        try:
            config = self._runtimes[name]
        except KeyError as error:
            raise ValidationError(f"runtime is not configured: {name}") from error
        if not config.enabled:
            raise ValidationError(f"runtime is disabled: {name}")
        adapter = load_adapter(config, required_capabilities)
        info = adapter.describe()
        if info.name != name:
            raise ValidationError(
                f"adapter {config.adapter} describes runtime {info.name!r}, expected {name!r}"
            )
        return adapter
