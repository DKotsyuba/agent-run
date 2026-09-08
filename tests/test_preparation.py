"""Focused checks for supervisor-owned preparation after READY."""

from __future__ import annotations

import json
import hashlib
import os
import threading
import time
from pathlib import Path

from agent_run import supervisor_main
from agent_run.domain import StartRequest
from agent_run.lifecycle import ReadyChannel
from agent_run.preparation import PreparationFailure, request_payload
from agent_run.role_plan import ResolvedRolePlan
from agent_run.state.store import StateStore


def test_ready_ownership_precedes_blocked_preparation_and_broker_close(tmp_path, monkeypatch) -> None:
    """Keep preparation owned and terminal after the broker connection disappears."""

    home = tmp_path.resolve()
    workdir = home / "work"
    workdir.mkdir()
    (home / "config.toml").write_text(
        f'''schema_version = 1
[runtimes.fake]
enabled = true
adapter = "test_service:ADAPTER"
binary = "/bin/true"
home = "{home / 'runtime'}"
models = ["model"]
''',
        encoding="utf-8",
    )
    request = StartRequest(
        "fake", "model", "profile", "task", workdir, timeout_seconds=480
    )
    role_payload = json.loads(
        (Path(__file__).parent / "fixtures" / "role_plan_7bbd43b.json").read_text(
            encoding="utf-8"
        )
    )
    role_payload["skills"] = []
    seed = {key: value for key, value in role_payload.items() if key != "config_revision"}
    role_payload["config_revision"] = hashlib.sha256(
        json.dumps(seed, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    role = ResolvedRolePlan.from_payload(role_payload)

    broker = StateStore.initialize(home / "state.db")
    creation = broker.create_agent_limited(
        request,
        task_summary="task",
        config_revision="pending:materialization",
        global_limit=2,
        runtime_limit=2,
        at=time.time(),
        startup_owner_identity=f"{os.getpid()} broker",
        startup_owner_birth_time=None,
        startup_deadline_seconds=120,
    )
    entered = threading.Event()
    release = threading.Event()

    def blocked(store, _home, _config, agent_id, _request, _role):
        """Observe durable supervisor identity, then fail materialization."""

        row = store.get_agent(agent_id)
        assert row["supervisor_pid"] == os.getpid()
        assert row["process_group_id"] == os.getpid()
        entered.set()
        assert release.wait(2)
        raise PreparationFailure("materialize", RuntimeError("materialize failed"))

    monkeypatch.setattr(supervisor_main, "prepare_launch", blocked)
    ready = ReadyChannel.open()
    thread = threading.Thread(
        target=supervisor_main._supervise,
        args=(
            {
                "agent_id": str(creation.agent_id),
                "request": request_payload(request),
                "role": role.to_payload(),
            },
            home,
            ready,
        ),
    )
    thread.start()
    assert ready.wait(1) == "ready"
    assert entered.wait(1)
    broker.close()
    release.set()
    thread.join(2)
    ready.close_read()
    ready.close_write()
    assert not thread.is_alive()
    observed = StateStore.open(home / "state.db")
    try:
        row = observed.get_agent(creation.agent_id)
        assert row["status"] == "failed"
        assert row["failure_kind"] == "prepare_materialize_failed"
    finally:
        observed.close()
