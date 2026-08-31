from __future__ import annotations

import base64
import json
from pathlib import Path


def account_email(auth_file: Path) -> str | None:
    try:
        payload = json.loads(auth_file.read_text())
        token = payload["tokens"]["id_token"]
        encoded = token.split(".")[1]
        encoded += "=" * (-len(encoded) % 4)
        claims = json.loads(base64.urlsafe_b64decode(encoded).decode("utf-8"))
        email = claims["email"]
        return email if isinstance(email, str) else None
    except Exception:
        return None


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
