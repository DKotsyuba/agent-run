"""Verified sealed-wheel deployment with quiescence and schema-safe recovery.

Only the maintainer release script calls this module. Deployment requires macOS,
an existing initialized home, a COMPLETE current release, and installed launchd
plists. Old releases and backups are retained; application hosts are never killed.
"""

from __future__ import annotations

import fcntl
import hashlib
import json
import math
import os
from pathlib import Path
import plistlib
import re
import select
import shutil
import socket
import sqlite3
import subprocess
import sys
import tempfile
import time

import psutil

from release import ReleaseError, Runner


def environment() -> dict[str, str]:
    """Return inherited string environment with imports prevented from mutating seals."""
    env = os.environ.copy()
    env.pop("PYTHONPATH", None)
    env.pop("PYTHONHOME", None)
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    return env


def python(runner: Runner, release: Path, source: str, *args: str) -> str:
    """Execute string source/args in sealed Path release Python; return stripped stdout."""
    return runner.run(str(release / "venv/bin/python"), "-I", "-B", "-c", source,
                      *args, env=environment()).stdout.strip()


def identity(runner: Runner, release: Path) -> tuple[str, int]:
    """Return installed (version string, supported schema int), raising on bad installs."""
    result = json.loads(python(runner, release,
        "import json; from importlib.metadata import version; "
        "from agent_run.state.migrations import SCHEMA_VERSION; "
        "print(json.dumps([version('agent-run'), SCHEMA_VERSION]))"))
    return result[0], result[1]


def verify_complete(release: Path) -> None:
    """Verify legacy-compatible COMPLETE and every manifest file in Path release.

    Reject missing/duplicate/traversing entries or mismatched hashes. Normalize
    legacy ./ paths and permit an omitted external Python symlink; when listed,
    interpreter symlinks are hashed by content. Package and CLI files are required.
    """
    if (release / "COMPLETE").read_text() != "complete\n":
        raise ReleaseError(f"Incomplete release: {release}")
    seen = set()
    for line in (release / "SHA256SUMS").read_text().splitlines():
        match = re.fullmatch(r"([0-9a-f]{64})  (.+)", line)
        if not match:
            raise ReleaseError(f"Invalid sealed manifest: {release}")
        relative = Path(match[2])
        name = relative.as_posix()
        if relative.is_absolute() or ".." in relative.parts or name in seen:
            raise ReleaseError("Unsafe or duplicate sealed manifest path")
        seen.add(name)
        if hashlib.sha256((release / relative).read_bytes()).hexdigest() != match[1]:
            raise ReleaseError(f"Corrupt sealed release file: {name}")
    if "venv/bin/agent-run" not in seen or not any(name.endswith("/site-packages/agent_run/__init__.py") for name in seen):
        raise ReleaseError("Sealed manifest does not cover runtime entry points")
    interpreter = release / "venv/bin/python"
    if not interpreter.is_file() or ("venv/bin/python" not in seen and not interpreter.is_symlink()):
        raise ReleaseError("Unverified sealed interpreter")


def rpc(home: Path, method: str) -> object:
    """Call read-only string method on Path home's socket; return JSON result or raise."""
    with socket.socket(socket.AF_UNIX) as connection:
        connection.settimeout(5)
        connection.connect(str(home / "api.sock"))
        connection.sendall((json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": {}}) + "\n").encode())
        with connection.makefile("rb") as stream:
            response = json.loads(stream.readline(1024 * 1024))
        if response.get("id") != 1 or "error" in response or "result" not in response:
            raise ReleaseError(f"API {method} failed")
        return response["result"]


def mcp_tools(runner: Runner, command: list[str], stderr: object) -> list[object]:
    """Return MCP tools while keeping child stdin open until the reply arrives.

    ``runner`` supplies the release deadline, ``command`` is the isolated
    agent-run argv prefix, and ``stderr`` receives child diagnostics. The
    handshake is capped at ten seconds and rejects EOF, oversized or malformed
    frames, protocol errors, and empty inventories. The owned child is always
    terminated and reaped before returning or raising.
    """
    process = subprocess.Popen(
        command + ["mcp"], env=environment(), stdin=subprocess.PIPE,
        stdout=subprocess.PIPE, stderr=stderr,
    )
    try:
        if process.stdin is None or process.stdout is None:
            raise ReleaseError("Isolated MCP stdio is unavailable")
        requests = (
            {"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {
                "protocolVersion": "2024-11-05", "capabilities": {},
                "clientInfo": {"name": "release-smoke", "version": "1"}}},
            {"jsonrpc": "2.0", "method": "notifications/initialized"},
            {"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}},
        )
        process.stdin.write("".join(json.dumps(request) + "\n" for request in requests).encode())
        process.stdin.flush()
        deadline = min(runner.deadline, time.monotonic() + 10)
        buffer = bytearray()
        initialized = False
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            ready, _, _ = select.select([process.stdout.fileno()], [], [], remaining)
            if not ready:
                break
            chunk = os.read(process.stdout.fileno(), 8192)
            if not chunk:
                break
            buffer.extend(chunk)
            while b"\n" in buffer:
                frame, _, tail = buffer.partition(b"\n")
                buffer = bytearray(tail)
                if len(frame) > 1024 * 1024:
                    raise ReleaseError("Isolated MCP returned an oversized frame")
                try:
                    message = json.loads(frame)
                except (UnicodeDecodeError, json.JSONDecodeError) as error:
                    raise ReleaseError("Isolated MCP returned malformed JSON") from error
                if not isinstance(message, dict):
                    raise ReleaseError("Isolated MCP returned malformed JSON")
                if "error" in message:
                    raise ReleaseError("Isolated MCP returned a protocol error")
                if message.get("id") == 1:
                    if not isinstance(message.get("result"), dict):
                        raise ReleaseError("Isolated MCP initialization failed")
                    initialized = True
                elif message.get("id") == 2:
                    result = message.get("result")
                    tools = result.get("tools") if isinstance(result, dict) else None
                    if not initialized or not isinstance(tools, list) or not tools:
                        raise ReleaseError("Isolated MCP tools/list smoke failed")
                    return tools
            if len(buffer) > 1024 * 1024:
                raise ReleaseError("Isolated MCP returned an oversized frame")
        raise ReleaseError("Isolated MCP tools/list smoke failed")
    finally:
        if process.stdin is not None:
            try:
                process.stdin.close()
            except OSError:
                pass
        if process.poll() is None:
            process.terminate()
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=10)
        if process.stdout is not None:
            process.stdout.close()


def smoke(runner: Runner, release: Path) -> None:
    """Test sealed Path release with isolated init/doctor/API/MCP; stop only owned API."""
    with tempfile.TemporaryDirectory(prefix="ar-smoke-", dir="/tmp") as directory:
        home = Path(directory)
        command = [str(release / "venv/bin/agent-run"), "--home", str(home)]
        runner.run(*command, "init", env=environment())
        runner.run(*command, "doctor", env=environment())
        with tempfile.TemporaryFile() as log:
            process = subprocess.Popen(command + ["api", "serve"], env=environment(), stdout=log, stderr=log)
            try:
                while True:
                    if process.poll() is not None:
                        raise ReleaseError("Isolated API exited during smoke")
                    try:
                        if rpc(home, "ping") != {"ok": True} or not rpc(home, "tools"):
                            raise ReleaseError("Isolated API returned empty tools")
                        break
                    except (OSError, ValueError):
                        runner.pause("isolated API readiness")
                mcp_tools(runner, command, log)
            finally:
                process.terminate()
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=10)


def install_locked_dependencies(runner: Runner, release: Path, requirements: Path) -> None:
    """Install the hash-pinned dependency closure into a sealed release venv.

    Requirements must enumerate every runtime dependency as a binary wheel with
    hashes. Pip rejects incomplete or altered closures before the application
    wheel is installed.
    """
    runner.run(str(release / "venv/bin/python"), "-I", "-m", "pip", "install",
               "--require-hashes", "--only-binary=:all:", "-r", str(requirements), env=environment())


def prepare(runner: Runner, target: Path, wheel: Path, requirements: Path, version: str, executable: str) -> int:
    """Reuse or build Path target from verified wheel and locked requirements.

    String executable must be Python 3.14. Incomplete candidates are preserved
    under timestamped names before rebuilding; corrupt COMPLETE candidates fail.
    COMPLETE is written only after isolated smoke and ``pip check`` confirms
    that the installed wheel's runtime dependencies are present.
    """
    if target.exists() and not (target / "COMPLETE").exists():
        target.rename(target.with_name(f"{target.name}.incomplete-{time.time_ns()}"))
    if target.exists():
        verify_complete(target)
    else:
        result = runner.run(executable, "-I", "-c", "import sys; print('%d.%d' % sys.version_info[:2])").stdout.strip()
        if result != "3.14":
            raise ReleaseError("Local releases require Python 3.14")
        target.mkdir(parents=True)
        runner.run(executable, "-m", "venv", str(target / "venv"))
        install_locked_dependencies(runner, target, requirements)
        runner.run(str(target / "venv/bin/python"), "-I", "-m", "pip", "install", "--no-deps", str(wheel), env=environment())
        runner.run(str(target / "venv/bin/python"), "-I", "-m", "pip", "check", env=environment())
        if identity(runner, target)[0] != version:
            raise ReleaseError("Installed wheel version mismatch")
        smoke(runner, target)
        files = sorted(path for path in target.rglob("*") if path.is_file())
        (target / "SHA256SUMS").write_text("".join(
            f"{hashlib.sha256(path.read_bytes()).hexdigest()}  {path.relative_to(target)}\n" for path in files))
        (target / "COMPLETE").write_text("complete\n")
        verify_complete(target)
    actual, schema = identity(runner, target)
    if actual != version:
        raise ReleaseError(f"Prepared release version {actual} differs from {version}")
    smoke(runner, target)
    return schema


def _legacy_writer_may_be_live(owner: object, birth: object) -> bool:
    """Return false only when an archived writer is provably gone or reused.

    ``owner`` begins with the historical writer PID and ``birth`` is its
    optional psutil creation time. Missing birth evidence can prove only PID
    absence; malformed identity, access denial, and other observation failures
    stay conservative and block migration.
    """

    try:
        pid = int(str(owner).partition(" ")[0])
        if pid <= 1:
            return True
    except (ValueError, TypeError):
        return True
    try:
        process = psutil.Process(pid)
        if birth is not None:
            expected_birth = float(birth)
            if expected_birth < 0 or not math.isfinite(expected_birth):
                return True
            if process.create_time() != expected_birth:
                return False
            if process.status() == psutil.STATUS_ZOMBIE:
                return False
        return process.is_running()
    except psutil.NoSuchProcess:
        return False
    except (PermissionError, ValueError, TypeError, psutil.Error):
        return True


def active(connection: sqlite3.Connection) -> int:
    """Count active agents and plausibly live archived workflow writers.

    Historical rows with no owner or a proven dead/reused process do not block.
    A matching writer, missing birth evidence, or unobservable process does,
    preventing schema migration while an old runtime may still write.
    """

    agents = connection.execute(
        "SELECT count(*) FROM agents WHERE status IN ('created','starting','running','cancelling')"
    ).fetchone()[0]
    columns = {
        str(row[1]) for row in connection.execute("PRAGMA table_info(workflow_runs)")
    }
    if not {"status", "owner_pid_identity"} <= columns:
        return agents
    birth = "owner_birth_time" if "owner_birth_time" in columns else "NULL"
    rows = connection.execute(
        f"""SELECT owner_pid_identity, {birth} FROM workflow_runs
            WHERE status IN ('created', 'running')
              AND owner_pid_identity IS NOT NULL"""
    )
    return agents + sum(
        _legacy_writer_may_be_live(owner, observed_birth)
        for owner, observed_birth in rows
    )


def database(home: Path) -> sqlite3.Connection:
    """Open existing Path home's SQLite database read-only, failing for missing stores."""
    path = home / "state.db"
    if not path.is_file():
        raise ReleaseError(f"State database is missing: {path}")
    return sqlite3.connect(path.as_uri() + "?mode=ro", uri=True, timeout=5)


def schema_version(home: Path, runner: Runner | None = None, *, integrity: bool = False) -> int:
    """Read integer schema from Path home, optionally verifying integrity on the same handle.

    Optional Runner retries only SQLite CANTOPEN/BUSY/LOCKED operational failures
    within its deadline, such as WAL sidecar recreation during daemon startup.
    Without runner, reads fail immediately. Missing files, corrupt databases and
    failed integrity checks always fail; no version is guessed from the journal.
    """
    while True:
        connection = None
        try:
            connection = database(home)
            version = connection.execute("PRAGMA user_version").fetchone()[0]
            if integrity and connection.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
                raise ReleaseError("SQLite integrity check failed")
            return version
        except sqlite3.OperationalError as error:
            code = getattr(error, "sqlite_errorcode", None)
            transient = (code is not None and code & 255 in (sqlite3.SQLITE_CANTOPEN, sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED))
            if code is None:
                transient = str(error) in ("unable to open database file", "database is locked", "database table is locked")
            if runner is None or not transient:
                raise
            if connection is not None:
                connection.close()
                connection = None
            runner.pause("SQLite readiness")
        finally:
            if connection is not None:
                connection.close()


def save_journal(path: Path, data: dict) -> None:
    """Atomically persist JSON dict data at Path path, with private permissions/fsync."""
    temporary = path.with_suffix(".tmp")
    with temporary.open("w") as stream:
        os.chmod(temporary, 0o600)
        json.dump(data, stream)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)


def switch(current: Path, target: Path) -> None:
    """Atomically replace Path current symlink with COMPLETE Path target; never delete it."""
    verify_complete(target)
    temporary = current.with_name(f".current-{os.getpid()}")
    if temporary.is_symlink():
        temporary.unlink()
    temporary.symlink_to(target)
    os.replace(temporary, current)


def loaded(runner: Runner, label: str) -> bool:
    """Return whether string launchd label is loaded in current user's GUI domain."""
    result = runner.run("launchctl", "print", f"gui/{os.getuid()}/{label}", check=False)
    if result.returncode and not any(message in result.stderr for message in ("Could not find service", "Could not find specified service")):
        raise ReleaseError(f"Cannot inspect launchd {label}: {result.stderr}")
    return result.returncode == 0


def restart(runner: Runner, jobs: dict[str, str]) -> None:
    """Restore every label/plist in string dict jobs, attempting all even on failure."""
    errors = []
    for label, plist in jobs.items():
        try:
            if not loaded(runner, label):
                runner.run("launchctl", "bootstrap", f"gui/{os.getuid()}", plist)
            runner.run("launchctl", "kickstart", f"gui/{os.getuid()}/{label}")
        except (ReleaseError, OSError, subprocess.SubprocessError) as error:
            errors.append(f"{label}: {error}")
    if errors:
        raise ReleaseError("Service recovery incomplete: " + "; ".join(errors))


def migrate(runner: Runner, target: Path, home: Path) -> None:
    """Migrate existing Path home using target's installed StateStore, closing it afterward."""
    python(runner, target, "import sys; from agent_run.state.store import StateStore; "
           "store=StateStore.initialize(sys.argv[1]); store.close()", str(home / "state.db"))


def validate_live(runner: Runner, current: Path, home: Path, version: str, schema: int) -> None:
    """Wait for API startup, then verify actual version/schema, integrity and doctor.

    Path current/home identify the installed runtime/store; string version and
    int schema must match exactly. Runner bounds API and transient SQLite waits.
    """
    while True:
        try:
            if rpc(home, "ping") != {"ok": True} or not rpc(home, "tools"):
                raise ReleaseError("Live API health check failed")
            break
        except (OSError, ValueError):
            runner.pause("live API readiness")
    if identity(runner, current) != (version, schema) or schema_version(home, runner, integrity=True) != schema:
        raise ReleaseError("Live runtime version/schema mismatch")
    runner.run(str(current / "venv/bin/agent-run"), "--home", str(home), "doctor", env=environment())


def recover(runner: Runner, home: Path, journal: dict) -> None:
    """Recover pointer/services from dict journal using actual DB schema, never downgrade.

    Prefer previous binary only at exactly its supported schema. Otherwise finish
    migration to target and start compatible services. Errors retain the journal
    and backup paths for explicit operator recovery.
    """
    previous, target = Path(journal["previous"]), Path(journal["target"])
    version = schema_version(home, runner, integrity=True)
    if version == journal["previous_schema"]:
        compatible = previous
    else:
        if not journal["previous_schema"] < version <= journal["target_schema"]:
            raise ReleaseError("Database schema is outside the recorded upgrade; preserve backup and inspect manually")
        verify_complete(target)
        if version < journal["target_schema"]:
            migrate(runner, target, home)
        compatible = target
    current = home / "standalone/current"
    verify_complete(compatible)
    if current.resolve() != compatible:
        switch(current, compatible)
    restart(runner, journal["jobs"])


def deploy(runner: Runner, home: Path, wheel: Path, requirements: Path, version: str, sha: str,
           executable: str, prefix: str) -> None:
    """Deploy verified wheel and hash-locked requirements into existing Path home.

    A nonblocking flock serializes deploys. Wait read-only for active work, then
    hold a short SQLite write reservation while stopping API admission and the
    loaded periodic jobs. Recheck, back up, migrate, swap and restore services.
    Any exception attempts schema-compatible recovery and retains its journal.
    """
    if sys.platform != "darwin":
        raise ReleaseError("Local deployment requires macOS; use --publish-only elsewhere")
    if not re.fullmatch(r"[A-Za-z0-9_.-]+", prefix):
        raise ReleaseError("Invalid launchd prefix")
    standalone = home / "standalone"
    current = standalone / "current"
    target = standalone / "releases" / sha
    if not current.is_symlink():
        raise ReleaseError("Existing standalone/current symlink required")
    with (standalone / "deploy.lock").open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise ReleaseError("Another local deployment is active") from error
        verify_complete(current.resolve())
        journal_path = standalone / "deploy.json"
        if journal_path.exists():
            journal = json.loads(journal_path.read_text())
            if journal["target"] != str(target):
                raise ReleaseError(f"Resume unfinished deployment to {journal['target']} first")
            recover(Runner(120, runner.poll), home, journal)
        previous = current.resolve()
        verify_complete(previous)
        _, previous_schema = identity(runner, previous)
        print(f"Preparing verified local release {version}", flush=True)
        target_schema = prepare(runner, target, wheel, requirements, version, executable)
        live_schema = schema_version(home, runner)
        if live_schema > target_schema:
            raise ReleaseError("Refusing runtime/schema downgrade")
        if previous == target and live_schema == target_schema:
            validate_live(runner, current, home, version, target_schema)
            journal_path.unlink(missing_ok=True)
            return
        jobs = {}
        for suffix in ("api", "capacity", "delivery"):
            label = f"{prefix}.{suffix}"
            plist = Path.home() / "Library/LaunchAgents" / f"{label}.plist"
            contents = plistlib.loads(plist.read_bytes())
            command = contents.get("ProgramArguments", [])
            if contents.get("Label") != label or not command or Path(command[0]) != current / "venv/bin/agent-run":
                raise ReleaseError(f"Launchd plist does not use current runtime: {plist}")
            configured_home = (command[command.index("--home") + 1] if "--home" in command
                               else contents.get("EnvironmentVariables", {}).get("AGENT_RUN_HOME", str(Path.home() / ".agent-run")))
            if Path(configured_home).expanduser().resolve() != home:
                raise ReleaseError(f"Launchd plist targets another home: {plist}")
            if loaded(runner, label):
                jobs[label] = str(plist)
        if f"{prefix}.api" not in jobs:
            raise ReleaseError("API launchd job must be loaded before deployment")
        while True:
            connection = database(home)
            try:
                count = active(connection)
            finally:
                connection.close()
            if not count:
                break
            runner.pause(f"{count} active agents to finish")
        backup = standalone / "backups" / f"{time.time_ns()}-{sha}"
        backup.mkdir(parents=True, mode=0o700)
        journal = {"stage": "stopping", "previous": str(previous), "target": str(target),
                   "previous_schema": previous_schema, "target_schema": target_schema,
                   "backup": str(backup), "jobs": jobs}
        save_journal(journal_path, journal)
        try:
            # Start admission reserves this same SQLite writer lock.
            connection = sqlite3.connect(home / "state.db", timeout=5)
            try:
                connection.execute("BEGIN IMMEDIATE")
                if active(connection):
                    raise ReleaseError("Work arrived during drain; retry after it finishes")
                for label in jobs:
                    runner.run("launchctl", "bootout", f"gui/{os.getuid()}/{label}")
                if active(connection):
                    raise ReleaseError("Active work appeared while stopping services")
                connection.rollback()
            finally:
                connection.close()
            connection = database(home)
            try:
                if active(connection):
                    raise ReleaseError("Work appeared after service shutdown; migration refused")
                with sqlite3.connect(backup / "state.db") as snapshot:
                    connection.backup(snapshot)
                    if snapshot.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
                        raise ReleaseError("SQLite backup integrity check failed; migration refused")
            finally:
                connection.close()
            shutil.copy2(home / "config.toml", backup / "config.toml")
            (backup / "previous.txt").write_text(str(previous) + "\n")
            journal["stage"] = "migrating"
            save_journal(journal_path, journal)
            print(f"Migrating schema {previous_schema} -> {target_schema}; backup {backup}", flush=True)
            migrate(runner, target, home)
            journal["stage"] = "switching"
            save_journal(journal_path, journal)
            switch(current, target)
            restart(runner, jobs)
            validate_live(runner, current, home, version, target_schema)
            journal_path.unlink()
        except BaseException as error:
            try:
                # Recovery has its own bounded allowance even when the main deadline expired.
                recover(Runner(120, runner.poll), home, journal)
            except BaseException as recovery_error:
                raise ReleaseError(f"Deployment failed ({error}); recovery failed ({recovery_error}). "
                                   f"Services may be stopped. Preserve {backup}; resume using {journal_path}") from error
            raise ReleaseError(f"Deployment failed ({error}); compatible services restored. "
                               f"Backup retained at {backup}; rerun to resume") from error
