"""A real-process stub adapter the exec'd supervisor loads from a test home.

`launch` is the only method the detached supervisor calls; everything else exists
so the trusted adapter registry accepts the module.
"""

from __future__ import annotations

import subprocess

from agent_run.adapters.base import (
    ADAPTER_API_VERSION,
    LaunchPlan,
    ModelInfo,
    RuntimeHealth,
    RuntimeInfo,
)
from agent_run.domain import AgentStatus, Outcome


class StubEngineSession:
    owns_process_group = True

    def __init__(self, process: subprocess.Popen) -> None:
        self._process = process

    @property
    def pid(self) -> int | None:
        return self._process.pid

    def wait(self, timeout_seconds: float | None) -> Outcome | None:
        try:
            code = self._process.wait(timeout_seconds)
        except subprocess.TimeoutExpired:
            return None
        if code == 0:
            return Outcome(AgentStatus.SUCCEEDED, exit_code=0)
        return Outcome(
            AgentStatus.FAILED, exit_code=code, failure_kind="engine_exit"
        )

    def steer(self, text: str) -> None:
        raise NotImplementedError("stub engine is not steerable")

    def cancel(self, grace_seconds: float) -> None:
        self._process.terminate()


class StubEngineAdapter:
    def describe(self) -> RuntimeInfo:
        return RuntimeInfo("fake", ADAPTER_API_VERSION, frozenset())

    def validate(self, config) -> None:
        return None

    def materialize(self, config, home, *, mcp_servers, skills_root) -> str:
        return "cfg-1"

    def probe(self, config, home) -> RuntimeHealth:
        return RuntimeHealth(True, "1", True, None)

    def models(self, config, home):
        return (ModelInfo("model", "stub engine"),)

    def limits(self, config, home):
        return ()

    def prepare(self, request, profile, config, home, agent_dir, *, mcp_servers):
        raise AssertionError("the detached supervisor never prepares a plan")

    def launch(self, plan: LaunchPlan, sink) -> StubEngineSession:
        sink.event("stub_engine_launched", {"argv": list(plan.argv)})
        process = subprocess.Popen(
            list(plan.argv),
            cwd=str(plan.cwd),
            env=dict(plan.environment),
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
        )
        return StubEngineSession(process)


ADAPTER = StubEngineAdapter()
