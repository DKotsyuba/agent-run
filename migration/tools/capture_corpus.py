"""Regenerate the sanitized Python compatibility corpus.

Usage::

    PYTHONPATH=src python3.14 \
        migration/tools/capture_corpus.py

The command writes only ``tests/fixtures/baseline``.  It uses deterministic
timestamps, IDs, dummy credentials, and path placeholders; it never invokes a
runtime engine or reads an operator home.
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import sqlite3
import tempfile
import uuid
from pathlib import Path
from unittest.mock import patch

from agent_run.adapters.base import LaunchPlan
from agent_run.adapters.claude import adapter as claude_adapter
from agent_run.adapters.claude.adapter import ADAPTER as CLAUDE
from agent_run.adapters.codex.adapter import ADAPTER as CODEX
from agent_run.adapters.glm import adapter as glm_adapter
from agent_run.adapters.glm.adapter import ADAPTER as GLM
from agent_run.adapters.home import seal_answer
from agent_run.adapters.qwen import adapter as qwen_adapter
from agent_run.adapters.qwen.adapter import ADAPTER as QWEN
from agent_run.config import RuntimeAuthConfig, RuntimeConfig
from agent_run.delivery.base import DeliveryAttemptEvidence
from agent_run.domain import AgentStatus, Message, MessageRole, OrchestratorRef, Outcome, StartRequest
from agent_run.role_plan import resolve_role_plan
from agent_run.profiles import AgentProfile
from agent_run.state import StateStore, record_run_stats
from agent_run.state import db as state_db
from agent_run.state import store as state_store_module
from agent_run.state.migrations import _apply_one, pending_files
from agent_run.verify import (
    DEFAULT_SENTINEL,
    answer_proof_path,
    inspect_answer,
    load_answer_proof,
    read_answer_payload,
)

#: Repository root containing this script and the Python package under test.
ROOT = Path(__file__).resolve().parents[2]
#: Output directory owned by this corpus generator.
OUT = ROOT / "tests" / "fixtures" / "baseline"
#: Stable epoch used for all persisted fixture observations.
NOW = 1_759_000_000.0
#: Fixed UUID used only where an adapter creates a native session argument.
FIXED_UUID = uuid.UUID("00000000-0000-4000-8000-000000000001")


def canonical_json(value: object) -> str:
    """Return compact, sorted UTF-8 JSON for a fixture document."""

    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def write_json(path: Path, value: object) -> None:
    """Write one deterministic JSON document with a final newline."""

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(canonical_json(value) + "\n", encoding="utf-8")


def replace_paths(value: str, temporary: Path, home: Path | None = None) -> str:
    """Replace temporary roots and generated-home paths with corpus tokens."""

    result = value
    if home is not None:
        result = result.replace(str(home), "${HOME_ROOT}")
    result = result.replace(str(temporary), "${TEMP_ROOT}")
    return result.replace(str(ROOT), "${REPOSITORY_ROOT}")


def request(workdir: Path, runtime: str = "codex", *, write: bool = False, profile: str = "review", request_id: str | None = None, orchestrator: OrchestratorRef | None = None) -> StartRequest:
    """Build one deterministic persisted launch request rooted at ``workdir``."""

    return StartRequest(
        runtime=runtime,
        model={"codex": "gpt-5.6-sol", "claude": "sonnet", "glm": "glm-5.3", "qwen": "opencode/MiniMaxM3"}.get(runtime, "model"),
        profile=profile,
        task="Inspect the fixture repository and report only the requested result.",
        workdir=workdir,
        write=write,
        effort="medium" if runtime in {"codex", "claude", "glm"} else None,
        timeout_seconds=480.0,
        request_id=request_id,
        orchestrator=orchestrator,
    )


def add_agent(store: StateStore, agent_id: str, item: str, *, status: AgentStatus = AgentStatus.CREATED, at: float = NOW, ref: OrchestratorRef | None = None, parent: str | None = None) -> tuple[str, str | None]:
    """Create one agent and move it through real StateStore lifecycle APIs."""

    created = store.create_agent_limited(
        request(ROOT, request_id=f"fixture-{item}", orchestrator=ref),
        task_summary=f"fixture {item}",
        config_revision="fixture-config-v1",
        global_limit=100,
        runtime_limit=100,
        agent_id=agent_id,
        at=at,
        parent_agent_id=parent,
        identity_json=canonical_json({"account": "fixture", "profile": "review"}),
        startup_owner_identity=f"fixture-owner-{item}",
        startup_owner_birth_time=at - 0.5,
        startup_deadline_seconds=120,
    )
    attempt = store.create_attempt(agent_id, state="running", attempt_id=f"att_{item}", adapter_state={"fixture": item}, at=at + 1)
    store.transition(agent_id, AgentStatus.RUNNING, attempt_id=attempt, at=at + 2)
    if status is AgentStatus.RUNNING:
        return agent_id, attempt
    if status is AgentStatus.CANCELLING:
        store.transition(agent_id, AgentStatus.CANCELLING, attempt_id=attempt, at=at + 3)
        return agent_id, attempt
    if status is AgentStatus.CANCELLED:
        store.transition(agent_id, AgentStatus.CANCELLING, attempt_id=attempt, at=at + 3)
        store.transition(agent_id, AgentStatus.CANCELLED, attempt_id=attempt, outcome=Outcome(AgentStatus.CANCELLED, failure_kind="fixture_cancelled"), at=at + 4)
        store.finish_attempt(agent_id, attempt, state="cancelled", at=at + 5)
        return agent_id, attempt
    if status is AgentStatus.LOST:
        store.record_supervisor(agent_id, pid=4100 + int(item[-1], 16), identity=f"fixture-supervisor-{item}", process_group_id=5100 + int(item[-1], 16), birth_time=at + 1, at=at + 3)
        store.reconcile_reaped(agent_id, 4100 + int(item[-1], 16), checked_at=at + 4)
        return agent_id, attempt
    store.transition(agent_id, status, attempt_id=attempt, outcome=Outcome(status, exit_code=0 if status is AgentStatus.SUCCEEDED else 1, failure_kind=None if status is AgentStatus.SUCCEEDED else f"fixture_{status.value}", runtime_session_id=f"session-{item}" if status is AgentStatus.SUCCEEDED else None), at=at + 4)
    store.finish_attempt(agent_id, attempt, state=status.value, at=at + 5)
    return agent_id, attempt


def capture_answers() -> list[dict[str, object]]:
    """Create valid v2/v1 answer artifacts and all requested corruptions."""

    root = OUT / "answers"
    root.mkdir(parents=True, exist_ok=True)
    cases: list[dict[str, object]] = []

    def record(name: str, payload: bytes, *, proof: bool, mutation: str | None = None) -> None:
        """Write one answer case, apply an optional corruption, and verify it."""

        directory = root / "agents" / name
        directory.mkdir(parents=True, exist_ok=True)
        path = directory / "answer.md"
        for stale in (path, answer_proof_path(path), path.parent / ".answer-format", path.parent / "outside.md"):
            stale.unlink(missing_ok=True)
        if proof:
            size, digest = seal_answer(path, payload.decode("utf-8"))
        else:
            framed = payload + (b"" if payload.endswith(b"\n") else b"\n") + DEFAULT_SENTINEL.encode() + b"\n"
            path.write_bytes(framed)
            size, digest = len(framed), hashlib.sha256(framed).hexdigest()
        if mutation == "sha_mismatch":
            path.write_bytes(b"X" + path.read_bytes()[1:])
        elif mutation == "size_mismatch":
            path.write_bytes(path.read_bytes() + b"!")
        elif mutation == "missing_proof":
            answer_proof_path(path).unlink()
        elif mutation == "truncated_payload":
            path.write_bytes(path.read_bytes()[:3])
        elif mutation == "symlinked_payload":
            target = directory / "outside.md"
            target.write_bytes(path.read_bytes())
            path.unlink()
            path.symlink_to(target)
        elif mutation == "invalid_utf8":
            path.write_bytes(b"\xff\xfe\x01")
            size, digest = 3, hashlib.sha256(b"\xff\xfe\x01").hexdigest()
        inspection: dict[str, object]
        try:
            inspected = inspect_answer(path)
            inspection = {"complete": inspected.complete, "evidence": inspected.evidence, "proof_version": inspected.proof_version, "proof_error": inspected.proof_error}
        except Exception as error:
            inspection = {"exception": type(error).__name__, "message": str(error)}
        try:
            load_answer_proof(path, expected_bytes=size, expected_sha256=digest)
            proof_result: dict[str, object] = {"verdict": "accepted"}
        except Exception as error:
            proof_result = {"exception": type(error).__name__, "message": str(error)}
        try:
            read_answer_payload(path, expected_bytes=size, expected_sha256=digest, max_bytes=1 << 20, strip_legacy=not proof)
            read_result: dict[str, object] = {"verdict": "accepted"}
        except Exception as error:
            read_result = {"exception": type(error).__name__, "message": str(error)}
        cases.append({"case": name, "format": 2 if proof else 1, "mutation": mutation, "read_path": f"${{CORPUS_ROOT}}/answers/agents/{name}/answer.md", "expected_bytes": size, "expected_sha256": digest, "inspection": inspection, "proof": proof_result, "read": read_result})

    record("proof-v2-valid", b"# Proof answer\n", proof=True)
    record("legacy-v1-valid", b"# Legacy answer\n", proof=False)
    for mutation in ("sha_mismatch", "size_mismatch", "missing_proof", "truncated_payload", "symlinked_payload", "invalid_utf8"):
        record(mutation, b"# Corruptible answer\n", proof=mutation != "invalid_utf8", mutation=mutation)
    write_json(root / "cases.json", {"version": 1, "cases": cases})
    return cases


def capture_database(answers: list[dict[str, object]], temporary: Path) -> dict[str, object]:
    """Build the current StateStore corpus and migration snapshots v2 through v15."""

    db_root = OUT / "db"
    db_root.mkdir(parents=True, exist_ok=True)
    current = db_root / "current-v16.sqlite"
    current.unlink(missing_ok=True)
    old_db_uuid = state_db.uuid.uuid4
    old_store_uuid = state_store_module.uuid.uuid4
    state_db.uuid.uuid4 = lambda: FIXED_UUID
    state_store_module.uuid.uuid4 = lambda: FIXED_UUID
    store = StateStore.initialize(current)
    ref = OrchestratorRef("codex_queue", "fixture-session", "turn-1")
    agents: dict[str, tuple[str, str | None]] = {}
    statuses = (AgentStatus.CREATED, AgentStatus.STARTING, AgentStatus.RUNNING, AgentStatus.CANCELLING, AgentStatus.SUCCEEDED, AgentStatus.FAILED, AgentStatus.TIMED_OUT, AgentStatus.CANCELLED, AgentStatus.LOST)
    for index, status in enumerate(statuses, 1):
        agent_id = f"ag-20260101-0000{index:02d}-000000000{index}"
        if status is AgentStatus.CREATED:
            created = store.create_agent(request(ROOT, request_id=f"fixture-created-{index}"), task_summary="fixture created", config_revision="fixture-config-v1", agent_id=agent_id, at=NOW + index)
            agents[status.value] = (str(created.agent_id), None)
        elif status is AgentStatus.STARTING:
            created = store.create_agent_limited(request(ROOT, request_id=f"fixture-starting-{index}"), task_summary="fixture starting", config_revision="fixture-config-v1", global_limit=100, runtime_limit=100, agent_id=agent_id, at=NOW + index, startup_owner_identity="fixture-start-owner", startup_owner_birth_time=NOW, startup_deadline_seconds=120)
            agents[status.value] = (str(created.agent_id), None)
        else:
            agents[status.value] = add_agent(store, agent_id, f"{index:02x}", status=status, at=NOW + index, ref=ref if status is AgentStatus.SUCCEEDED else None)
    running, attempt = agents["running"]
    store.append_message(running, Message(NOW + 20, MessageRole.USER, "fixture request"), attempt_id=attempt)
    store.append_message(running, Message(NOW + 21, MessageRole.ASSISTANT, "fixture response"), attempt_id=attempt)
    store.enqueue_command(running, "cancel", {"reason": "operator requested"}, at=NOW + 22)
    store.enqueue_command(running, "steer", {"text": "continue with the bounded task"}, at=NOW + 23)
    cancelling, cancelling_attempt = agents["cancelling"]
    store.enqueue_command(cancelling, "cancel", {"reason": "fixture"}, at=NOW + 24)
    succeeded, succeeded_attempt = agents["succeeded"]
    store.append_event(succeeded, "runtime_result", attempt_id=succeeded_attempt, at=NOW + 24, data={"usage": {"input_tokens": 10, "output_tokens": 20}, "num_turns": 1, "total_cost_usd": 0.01})
    answer_path = OUT / "answers" / "agents" / "proof-v2-valid" / "answer.md"
    digest = str(next(item["expected_sha256"] for item in answers if item["case"] == "proof-v2-valid"))
    store.connection.execute("UPDATE agents SET answer_path=?, answer_bytes=?, answer_sha256=? WHERE id=?", ("${CORPUS_ROOT}/answers/agents/proof-v2-valid/answer.md", 15, digest, succeeded))
    delivery_row = store.connection.execute("SELECT id FROM deliveries WHERE agent_id=?", (succeeded,)).fetchone()
    if delivery_row:
        store.connection.execute("UPDATE deliveries SET id='delivery-succeeded' WHERE id=?", (delivery_row["id"],))
        store.connection.commit()
        evidence = DeliveryAttemptEvidence("retry", "codex-queue", ("codex-queue", "<redacted>"), 12, returncode=75, error_class="temporary", stderr_tail="retryable", stderr_bytes=9)
        claimed = store.claim_delivery("fixture-dispatcher", at=NOW + 25)
        store.retry_delivery(str(claimed["id"]), "fixture-dispatcher", "temporary failure", evidence=evidence, at=NOW + 26, base_delay=1, max_delay=10)
        claimed = store.claim_delivery("fixture-dispatcher", at=NOW + 28)
        delivered = DeliveryAttemptEvidence("delivered", "codex-queue", ("codex-queue", "<redacted>"), 8, returncode=0, message_id_present=True, stdout_tail="ok", stdout_bytes=2)
        store.complete_delivery(str(claimed["id"]), "fixture-dispatcher", remote_message_id="remote-fixture", evidence=delivered, at=NOW + 29)
    parent, parent_attempt = add_agent(store, "ag-20260101-000010-0000000001", "0a", status=AgentStatus.SUCCEEDED, at=NOW + 30)
    child, child_attempt = add_agent(store, "ag-20260101-000011-0000000001", "0b", status=AgentStatus.RUNNING, at=NOW + 31, parent=parent)
    store.append_event(child, "resume_attached", attempt_id=child_attempt, at=NOW + 34, data={"parent_agent_id": parent, "runtime_session_id": "session-parent"})
    store.append_capacity_samples(({"runtime": "codex", "lane": "primary", "window": "session_5h", "source": "fixture", "remaining_percent": 82.0, "observed_at": NOW + 35, "valid_until": NOW + 335, "payload": {"remaining": 82}},), runtime="codex", scope_id="fixture-account", observed_at=NOW + 35, valid_until=NOW + 335, payload={"route": "fixture", "models": ["gpt-5.6-sol"]})
    store.connection.execute("INSERT INTO reconciliation_cursors(name, created_at, agent_id) VALUES ('fixture-cursor', ?, ?)", (NOW + 36, running))
    store.connection.commit()
    record_run_stats(store, succeeded, at=NOW + 37)
    record_run_stats(store, child, at=NOW + 38)
    store.close()
    state_db.uuid.uuid4 = old_db_uuid
    state_store_module.uuid.uuid4 = old_store_uuid
    current.with_name(f".{current.name}.init.lock").unlink(missing_ok=True)

    v2 = temporary / "state-v2.sqlite"
    connection = sqlite3.connect(v2)
    connection.executescript((ROOT / "tests" / "fixtures" / "schema_v2.sql").read_text(encoding="utf-8"))
    connection.execute("INSERT INTO agents (id,runtime,model,profile,task,task_summary,workdir,request_json,status,created_at,timeout_seconds,config_revision) VALUES ('ag-20260101-000099-0000000001','codex','gpt-5.6-sol','review','legacy task','legacy summary','/fixture','{}','running',1,480,'legacy-v2')")
    connection.execute("INSERT INTO events (agent_id,at,kind) VALUES ('ag-20260101-000099-0000000001',2,'created')")
    connection.execute("INSERT INTO workflow_runs (id,name,script_sha,status,created_at) VALUES ('workflow-v2','fixture','sha256','failed',3)")
    connection.execute("INSERT INTO workflow_steps (run_id,step_key,spec_json,status) VALUES ('workflow-v2','step-1','{}','failed')")
    connection.commit()
    connection.close()
    shutil.copy2(v2, db_root / "historical-v2.sqlite")
    for target, source in pending_files():
        if target <= 2:
            continue
        if target > 15:
            break
        connection = sqlite3.connect(v2)
        _apply_one(connection, v2, target, source)
        connection.close()
        shutil.copy2(v2, db_root / f"historical-v{target}.sqlite")
    return manifest_for_databases(db_root)


def table_counts(path: Path) -> dict[str, int]:
    """Return sorted application-table row counts from one SQLite fixture."""

    connection = sqlite3.connect(path)
    try:
        names = [str(row[0]) for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name")]
        return {name: int(connection.execute(f'SELECT COUNT(*) FROM "{name}"').fetchone()[0]) for name in names}
    finally:
        connection.close()


def manifest_for_databases(db_root: Path) -> dict[str, object]:
    """Describe every generated database with version, digest, and row counts."""

    files = []
    for path in sorted(db_root.glob("*.sqlite")):
        connection = sqlite3.connect(path)
        version = int(connection.execute("PRAGMA user_version").fetchone()[0])
        connection.close()
        files.append({"file": path.name, "user_version": version, "sha256": hashlib.sha256(path.read_bytes()).hexdigest(), "rows": table_counts(path), "how_produced": "StateStore APIs and numbered Python migrations; sanitized deterministic fixture data."})
    return {"schema_version": 16, "files": files, "unbuildable_versions": []}


def build_role(workdir: Path, *, write: bool) -> object:
    """Resolve the canonical default-style review or implement role."""

    profile = AgentProfile("implement" if write else "review", "Canonical fixture role.", write, (), False, "default-catalog-v1", False, (), (), frozenset(), True)
    return resolve_role_plan(profile, skills_root=workdir, mcp_catalog={})


def tree_snapshot(home: Path, temporary: Path) -> dict[str, object]:
    """Capture generated home files, preserving symlink type and sanitized text."""

    entries: dict[str, object] = {}
    for path in sorted(home.rglob("*")):
        relative = path.relative_to(home).as_posix()
        if path.is_symlink():
            entries[relative] = {"type": "symlink", "target": replace_paths(os.readlink(path), temporary, home)}
        elif path.is_file():
            raw = path.read_bytes()
            try:
                content: object = replace_paths(raw.decode("utf-8"), temporary, home)
            except UnicodeDecodeError:
                content = {"encoding": "base64", "bytes": raw.hex()}
            entries[relative] = {"type": "file", "content": content}
    return entries


def capture_homes(temporary: Path) -> None:
    """Materialize and prepare both canonical role variants for four adapters."""

    homes = OUT / "homes"
    homes.mkdir(parents=True, exist_ok=True)
    workdir = temporary / "workdir"
    workdir.mkdir()
    auth = temporary / "codex-auth.json"
    auth.write_text('{"credential":"dummy"}\n', encoding="utf-8")
    configs = {
        "codex": RuntimeConfig(True, "agent_run.adapters.codex.adapter:ADAPTER", Path("/bin/echo"), temporary / "codex", ("gpt-5.6-sol",), auth=RuntimeAuthConfig("file_link", auth, "auth.json")),
        "claude": RuntimeConfig(True, "agent_run.adapters.claude.adapter:ADAPTER", Path("/bin/echo"), temporary / "claude", ("sonnet",), auth=RuntimeAuthConfig("environment", names=("CLAUDE_CODE_OAUTH_TOKEN",))),
        "glm": RuntimeConfig(True, "agent_run.adapters.glm.adapter:ADAPTER", Path("/bin/echo"), temporary / "glm", ("glm-5.3",), auth=RuntimeAuthConfig("environment", names=("ANTHROPIC_AUTH_TOKEN", "ANTHROPIC_BASE_URL"))),
        "qwen": RuntimeConfig(True, "agent_run.adapters.qwen.adapter:ADAPTER", Path("/bin/echo"), temporary / "qwen", ("opencode/MiniMaxM3",), auth=RuntimeAuthConfig("environment", names=("OPENAI_API_KEY", "OPENAI_BASE_URL"))),
    }
    os.environ.update({"CLAUDE_CODE_OAUTH_TOKEN": "dummy-claude-token", "ANTHROPIC_AUTH_TOKEN": "dummy-glm-token", "OPENAI_API_KEY": "dummy-openai-key", "OPENAI_BASE_URL": "http://127.0.0.1:20128/v1"})
    original_glm_keychain = glm_adapter.keychain_glm_key
    glm_adapter.keychain_glm_key = lambda: None
    try:
        for runtime, config in configs.items():
            adapter = {"codex": CODEX, "claude": CLAUDE, "glm": GLM, "qwen": QWEN}[runtime]
            for variant, writable in (("read-only", False), ("write", True)):
                output_dir = homes / runtime / variant
                output_dir.mkdir(parents=True, exist_ok=True)
                home = temporary / "generated-homes" / runtime / variant
                home.mkdir(parents=True)
                role = build_role(workdir, write=writable)
                req = request(workdir, runtime, write=writable, profile="implement" if writable else "review")
                agent_dir = temporary / "agents" / runtime / variant
                agent_dir.mkdir(parents=True)
                if runtime == "codex":
                    (home / "cache").mkdir(parents=True)
                    (home / "cache" / "models.json").write_text('{"models":[{"id":"gpt-5.6-sol","description":"fixture","efforts":["medium"]}]}\n', encoding="utf-8")
                materialize = getattr(adapter, "materialize")
                materialize(config, home, mcp_servers={})
                with patch.object(claude_adapter.uuid, "uuid4", return_value=FIXED_UUID):
                    plan: LaunchPlan = adapter.prepare(req, role, config, home, agent_dir)
                payload = plan.to_payload()
                argv = [replace_paths(str(item), temporary, home) for item in payload["argv"]]
                environment_names = sorted(str(name) for name in plan.environment)
                safe_values: dict[str, str] = {}
                for name in ("HOME", "CODEX_HOME", "CLAUDE_CONFIG_DIR"):
                    if name in plan.environment:
                        safe_values[name] = "${HOME_ROOT}"
                for name in ("OPENAI_MODEL", "OPENAI_BASE_URL", "ANTHROPIC_MODEL"):
                    if name in plan.environment:
                        safe_values[name] = replace_paths(str(plan.environment[name]), temporary, home)
                secret_names = sorted(str(item) for item in plan.adapter_state.get("secret_env_names", ()))
                write_json(output_dir / "launch.json", {"argv": argv, "cwd": "${WORKDIR}", "environment_names": environment_names, "environment_values": safe_values, "secret_env_names": secret_names, "adapter_state": replace_paths(canonical_json(dict(plan.adapter_state)), temporary, home)})
                write_json(output_dir / "tree.json", tree_snapshot(home, temporary))
    finally:
        glm_adapter.keychain_glm_key = original_glm_keychain


def protocol_index() -> list[dict[str, object]]:
    """Index recorded engine fixtures without copying them into the corpus."""

    indexed: list[dict[str, object]] = []
    for path in sorted((ROOT / "tests" / "fixtures").rglob("*")):
        if not path.is_file() or path.is_relative_to(OUT):
            continue
        if path.suffix not in {".jsonl", ".ndjson"} and "engine" not in path.name.lower():
            continue
        relative = path.relative_to(ROOT).as_posix()
        matches = []
        for test in sorted((ROOT / "tests").glob("test_*.py")):
            if path.name in test.read_text(encoding="utf-8", errors="ignore"):
                matches.append(test.relative_to(ROOT).as_posix())
        lower = relative.lower()
        engine = next((name for name in ("codex", "claude", "glm", "qwen") if name in lower), "unknown")
        indexed.append({"file": relative, "engine": engine, "python_tests": matches, "proves": "Recorded engine transcript consumed by Python adapter tests."})
    return indexed


def main() -> None:
    """Regenerate answers, databases, runtime homes, protocol index, and README."""

    OUT.mkdir(parents=True, exist_ok=True)
    temporary = (Path(tempfile.gettempdir()) / "agent-run-corpus").resolve()
    if temporary.exists():
        shutil.rmtree(temporary)
    temporary.mkdir(mode=0o700)
    try:
        answers = capture_answers()
        manifest = capture_database(answers, temporary)
        capture_homes(temporary)
    finally:
        shutil.rmtree(temporary)
    write_json(OUT / "db" / "manifest.json", manifest)
    write_json(OUT / "protocol" / "index.json", {"version": 1, "fixtures": protocol_index()})
    (OUT / "README-corpus.md").write_text(
        "# Python compatibility corpus\n\n"
        "Provenance: Python reference SHA `c9904f9843ba4a0772bdfa8bac5259f18fad9dc3`.\n\n"
        "Regenerate from the repository root with:\n\n"
        "```sh\nPYTHONPATH=src python3.14 migration/tools/capture_corpus.py\n```\n\n"
        "The corpus is generated by Python StateStore, migration, adapter, answer-sealing, and verification APIs. It contains fixed synthetic IDs/timestamps, dummy credentials only, no conversations or live-engine output, and replaces temporary paths with `${TEMP_ROOT}`, `${HOME_ROOT}`, `${CORPUS_ROOT}`, or `${WORKDIR}`. Engine transcripts already recorded under `tests/fixtures/` are indexed, not copied.\n",
        encoding="utf-8",
    )


if __name__ == "__main__":
    main()
