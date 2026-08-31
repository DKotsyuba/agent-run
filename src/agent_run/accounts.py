from __future__ import annotations

from pathlib import Path


def account_store_dir(agent_run_home: str | Path, runtime_name: str, label: str) -> Path:
    return Path(agent_run_home) / "accounts" / runtime_name / label


def account_auth_source(
    agent_run_home: str | Path,
    runtime_name: str,
    label: str,
    target: str = "auth.json",
) -> Path:
    return account_store_dir(agent_run_home, runtime_name, label) / Path(target).name


def account_runtime_home(runtime_home: str | Path, label: str) -> Path:
    home = Path(runtime_home)
    return home.with_name(home.name + "@" + label)
