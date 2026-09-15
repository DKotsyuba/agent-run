"""Build the pinned Python-to-Rust migration coverage ledger.

Usage::

    /Users/pluto/projects/agent-run/.venv-py314/bin/python \
        migration/tools/build_inventory.py

The command must run from the repository root.  It reads the Python baseline
from Git, maps source and test paths to the task board, detects current Rust
test names, and writes the four P0 coverage artifacts under
``migration/baseline``.  It has no network or runtime-engine dependencies.
"""

from __future__ import annotations

import argparse
import ast
import csv
import hashlib
import json
import re
import subprocess
import sys
from collections import Counter, defaultdict
from collections.abc import Iterable, Sequence
from pathlib import Path


#: Repository-relative commit containing the complete Python reference tree.
PYTHON_BASELINE = "c9904f9843ba4a0772bdfa8bac5259f18fad9dc3"
#: Repository root inferred from this development-only tool's location.
ROOT = Path(__file__).resolve().parents[2]
#: Directory containing generated P0 coverage artifacts.
OUT = ROOT / "migration" / "baseline"


def _task_key(task_id: str) -> tuple[int, str]:
    """Return a stable numeric sort key for task identifiers such as ``M13a``."""

    match = re.fullmatch(r"M(\d+)(.*)", task_id)
    return (int(match.group(1)), match.group(2)) if match else (10**9, task_id)


def _git_lines(*args: str) -> list[str]:
    """Run a read-only Git command and return non-empty UTF-8 lines."""

    result = subprocess.run(
        ["git", *args], cwd=ROOT, check=True, stdout=subprocess.PIPE, text=True
    )
    return [line for line in result.stdout.splitlines() if line]


def _baseline_bytes(path: str) -> bytes:
    """Read one baseline blob exactly as stored by Git, including binary data."""

    result = subprocess.run(
        ["git", "cat-file", "blob", f"{PYTHON_BASELINE}:{path}"],
        cwd=ROOT,
        check=True,
        stdout=subprocess.PIPE,
    )
    return result.stdout


def _baseline_paths() -> list[str]:
    """Return every regular file tracked by the pinned baseline, sorted by path."""

    return sorted(_git_lines("ls-tree", "-r", "--name-only", PYTHON_BASELINE))


def _kind(path: str) -> str:
    """Classify a baseline path using the manifest's stable vocabulary."""

    if path.startswith("tests/fixtures/"):
        return "test-fixture"
    if path.startswith("tests/") and path.endswith(".py"):
        return "python-test"
    if path.startswith("src/agent_run/operator_guide/"):
        return "operator-doc"
    if path.startswith("src/agent_run/") and path.endswith(".py"):
        return "python-src"
    if path.startswith("scripts/"):
        return "script"
    if path.startswith(".github/"):
        return "ci"
    if path.endswith(".sql"):
        return "sql"
    if path in {
        "pyproject.toml",
        "uv.lock",
        ".python-version",
        "pyrightconfig.json",
        "MANIFEST.in",
    }:
        return "packaging"
    if path.startswith("docs/"):
        return "doc"
    if path.endswith((".json", ".toml", ".cjs")) and path.startswith("src/"):
        return "asset"
    return "other"


def _purpose(path: str, content: bytes) -> str:
    """Return a concise purpose from a module docstring or a path fallback."""

    if path.endswith(".py"):
        try:
            module = ast.parse(content.decode("utf-8"), filename=path)
            docstring = ast.get_docstring(module, clean=True)
        except (SyntaxError, UnicodeDecodeError):
            docstring = None
        if docstring:
            first_line = " ".join(docstring.splitlines()).strip()
            if first_line:
                return first_line[:180]
    stem = Path(path).stem.replace("_", " ").replace("-", " ")
    if path.startswith("src/agent_run/operator_guide/"):
        return f"Operator guidance for {stem}."
    if path.endswith(".sql"):
        return f"SQLite schema or migration for {stem}."
    if path.endswith(".cjs"):
        return f"JavaScript transport asset for {stem}."
    if path.startswith("scripts/"):
        return f"Release or development script for {stem}."
    if path.startswith("tests/"):
        return f"Regression coverage for {stem}."
    return f"Project resource for {stem}."


def _module_name(path: str) -> str | None:
    """Convert a Python source path into its importable module name."""

    if not path.startswith("src/") or not path.endswith(".py"):
        return None
    parts = Path(path).with_suffix("").parts[1:]
    if parts[-1] == "__init__":
        parts = parts[:-1]
    return ".".join(parts)


def _intra_imports(path: str, content: bytes, modules: set[str]) -> list[str]:
    """Extract sorted imports that resolve inside the ``agent_run`` package."""

    if not path.startswith("src/") or not path.endswith(".py"):
        return []
    try:
        tree = ast.parse(content.decode("utf-8"), filename=path)
    except (SyntaxError, UnicodeDecodeError):
        return []
    current = _module_name(path) or ""
    package = current.split(".")[:-1]
    found: set[str] = set()
    for node in ast.walk(tree):
        candidates: list[str] = []
        if isinstance(node, ast.Import):
            candidates = [alias.name for alias in node.names]
        elif isinstance(node, ast.ImportFrom):
            if node.level:
                base = package[: max(0, len(package) - node.level + 1)]
                prefix = ".".join(base)
                candidates = [
                    ".".join(part for part in (prefix, node.module or name) if part)
                    for name in (alias.name for alias in node.names)
                ]
                if node.module is None:
                    candidates = [prefix]
            elif node.module:
                candidates = [
                    ".".join(part for part in (node.module, alias.name) if part)
                    for alias in node.names
                ]
        for candidate in candidates:
            if candidate == "agent_run" or candidate in modules:
                found.add(candidate)
            else:
                prefix = candidate
                while "." in prefix:
                    prefix = prefix.rsplit(".", 1)[0]
                    if prefix in modules:
                        found.add(prefix)
                        break
    return sorted(found)


#: Source ownership rules.  The preferred target is copied from a task's
#: target list, so the ledger remains aligned with the board's crate layout.
SOURCE_RULES: tuple[tuple[str, tuple[str, ...], str], ...] = (
    ("src/agent_run/state/migrations/", ("M18",), "sql/migrations/002-016"),
    ("src/agent_run/state/schema.sql", ("M17",), "sql/schema.sql"),
    ("src/agent_run/state/migrations.py", ("M18",), "crates/agent-run-store/src/migrations.rs"),
    ("src/agent_run/adapters/codex/defaults.toml", ("M32a",), "assets/codex/defaults.toml"),
    ("src/agent_run/delivery/completion_notice_contract.json", ("M47",), "assets/completion_notice.json"),
    ("src/agent_run/adapters/codex/", ("M31a", "M32a", "M32b", "M32c", "M34a", "M34b"), "crates/agent-run-adapters/src/codex/session.rs"),
    ("src/agent_run/adapters/claude/", ("M35a", "M35b", "M35c"), "crates/agent-run-adapters/src/claude"),
    ("src/agent_run/adapters/glm/", ("M36",), "crates/agent-run-adapters/src/glm"),
    ("src/agent_run/adapters/qwen/", ("M37",), "crates/agent-run-adapters/src/qwen"),
    ("src/agent_run/adapters/", ("M13a", "M14a", "M14b", "M25", "M38"), "crates/agent-run-adapters/src/lib.rs"),
    ("src/agent_run/capacity/", ("M20d", "M43a", "M43b", "M43c", "M43d", "M44", "M45"), "crates/agent-run-core/src/capacity/mod.rs"),
    ("src/agent_run/state/diagnostics.py", ("M20c",), "crates/agent-run-store/src/diagnostics.rs"),
    ("src/agent_run/state/reconciliation.py", ("M30a",), "crates/agent-run-store/src/reconciliation.rs"),
    ("src/agent_run/state/resume.py", ("M22", "M38"), "crates/agent-run-store/src/lineage.rs"),
    ("src/agent_run/state/run_stats.py", ("M45",), "crates/agent-run-store/src/run_stats.rs"),
    ("src/agent_run/state/capacity.py", ("M20d",), "crates/agent-run-store/src/capacity.rs"),
    ("src/agent_run/state/delivery.py", ("M21", "M46"), "crates/agent-run-store/src/delivery.rs"),
    ("src/agent_run/state/", ("M17", "M19", "M20a", "M20b", "M21", "M22", "M28a"), "crates/agent-run-store/src/lib.rs"),
    ("src/agent_run/delivery/", ("M46", "M47", "M48"), "crates/agent-run-core/src/delivery/mod.rs"),
    ("src/agent_run/hooks/", ("M49",), "crates/agent-run/src/hooks"),
    ("src/agent_run/process_identity.py", ("M07", "M26"), "crates/agent-run-platform/src/process.rs"),
    ("src/agent_run/launch_evidence.py", ("M28b",), "crates/agent-run-platform/src/launch_evidence.rs"),
    ("src/agent_run/verify.py", ("M24",), "crates/agent-run-platform/src/answer.rs"),
    ("src/agent_run/fs.py", ("M23a", "M23b"), "crates/agent-run-platform/src/fs.rs"),
    ("src/agent_run/paths.py", ("M14a", "M23a"), "crates/agent-run-platform/src/paths.rs"),
    ("src/agent_run/launch.py", ("M06", "M28b"), "crates/agent-run-core/src/launch.rs"),
    ("src/agent_run/preparation.py", ("M28c",), "crates/agent-run-core/src/preparation.rs"),
    ("src/agent_run/lifecycle.py", ("M26", "M29", "M30b"), "crates/agent-run-core/src/lifecycle.rs"),
    ("src/agent_run/supervisor", ("M28b", "M30c"), "crates/agent-run-core/src/supervisor.rs"),
    ("src/agent_run/wait.py", ("M30c",), "crates/agent-run-core/src/wait.rs"),
    ("src/agent_run/native_settings.py", ("M13b", "M16"), "crates/agent-run-config/src/native_settings.rs"),
    ("src/agent_run/role_plan.py", ("M15a",), "crates/agent-run-config/src/role_plan.rs"),
    ("src/agent_run/effective_policy.py", ("M15b",), "crates/agent-run-domain/src/policy.rs"),
    ("src/agent_run/profiles.py", ("M15b",), "crates/agent-run-config/src/profiles.rs"),
    ("src/agent_run/accounts.py", ("M14a",), "crates/agent-run-config/src/accounts.rs"),
    ("src/agent_run/config.py", ("M13a",), "crates/agent-run-config/src/lib.rs"),
    ("src/agent_run/domain.py", ("M11",), "crates/agent-run-domain/src/types.rs"),
    ("src/agent_run/errors.py", ("M11",), "crates/agent-run-domain/src/error.rs"),
    ("src/agent_run/dispatch.py", ("M12",), "crates/agent-run-domain/src/tools.rs"),
    ("src/agent_run/service.py", ("M19", "M30d"), "crates/agent-run-core/src/service.rs"),
    ("src/agent_run/resume.py", ("M38",), "crates/agent-run-core/src/resume.rs"),
    ("src/agent_run/api_socket.py", ("M39", "M40"), "crates/agent-run/src/transport/socket.rs"),
    ("src/agent_run/broker_client.py", ("M39", "M42"), "crates/agent-run/src/transport/socket.rs"),
    ("src/agent_run/mcp.py", ("M10", "M42"), "crates/agent-run/src/transport/mcp.rs"),
    ("src/agent_run/cli.py", ("M41a",), "crates/agent-run/src/cli.rs"),
    ("src/agent_run/api_launchd.py", ("M51",), "crates/agent-run/src/launchd.rs"),
    ("src/agent_run/doctor.py", ("M50a",), "crates/agent-run/src/doctor.rs"),
    ("src/agent_run/doc.py", ("M50b",), "crates/agent-run/src/doc.rs"),
    ("src/agent_run/logging_setup.py", ("M50a",), "crates/agent-run/src/logging.rs"),
    ("src/agent_run/operator_guide/", ("M50b",), "assets/operator_guide"),
    ("scripts/", ("M52",), "xtask/src/release.rs"),
    ("src/agent_run/", ("M11",), "crates/agent-run-domain/src/lib.rs"),
)


def _source_owner(path: str, valid_tasks: set[str]) -> tuple[list[str], str]:
    """Find the first matching source rule and validate its task identifiers."""

    for prefix, task_ids, target in SOURCE_RULES:
        if path == prefix or path.startswith(prefix):
            unknown = set(task_ids) - valid_tasks
            if unknown:
                raise ValueError(f"unknown task IDs for {path}: {sorted(unknown)}")
            return list(task_ids), target
    return ["M02"], "migration/baseline/inventory.json"


def _resource_paths(path: str, content: bytes, all_paths: Sequence[str]) -> list[str]:
    """Find package resources named by one source file without reading runtime state."""

    if not path.startswith("src/agent_run/") or not path.endswith(".py"):
        return []
    text = content.decode("utf-8", errors="ignore")
    resources = []
    for candidate in all_paths:
        if not candidate.startswith("src/agent_run/") or candidate.endswith(".py"):
            continue
        basename = Path(candidate).name
        if basename in text or Path(candidate).parent.name in text and Path(candidate).suffix in text:
            resources.append(candidate)
    if "operator_guide" in text:
        resources.extend(
            candidate for candidate in all_paths if candidate.startswith("src/agent_run/operator_guide/")
        )
    if "migrations" in text:
        resources.extend(
            candidate for candidate in all_paths if candidate.startswith("src/agent_run/state/migrations/")
        )
    return sorted(set(resources))


def _load_tasks() -> tuple[list[dict[str, str]], dict[str, dict[str, str]]]:
    """Read the task board and return ordered rows plus an ID lookup."""

    with (ROOT / "migration" / "tasks.csv").open(newline="", encoding="utf-8") as stream:
        rows = list(csv.DictReader(stream))
    lookup = {row["id"]: row for row in rows}
    return rows, lookup


def _expand_tests(spec: str) -> list[str]:
    """Expand semicolon-separated plan ranges such as ``T55-T57``."""

    result: set[str] = set()
    for token in filter(None, (part.strip() for part in spec.split(";"))):
        match = re.fullmatch(r"T(\d+)-T(\d+)", token)
        if match:
            result.update(f"T{number:02d}" for number in range(int(match.group(1)), int(match.group(2)) + 1))
        elif re.fullmatch(r"T\d+", token):
            result.add(token)
    return sorted(result, key=lambda item: int(item[1:]))


def _plan_tests(task_ids: Iterable[str], tasks: dict[str, dict[str, str]]) -> list[str]:
    """Combine and numerically sort plan test IDs for a task selection."""

    tests = {test for task_id in task_ids for test in _expand_tests(tasks[task_id]["plan_tests"])}
    return sorted(tests, key=lambda item: int(item[1:]))


TEST_RULES: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("tests/test_adapter", ("M13a", "M14a", "M14b", "M25")),
    ("tests/test_adapters_base.py", ("M13a", "M31a")),
    ("tests/test_answer_payload_proof.py", ("M24", "M30b")),
    ("tests/test_api_socket.py", ("M39", "M40")),
    ("tests/test_bind_hook.py", ("M49",)),
    ("tests/test_broker_client.py", ("M39", "M42")),
    ("tests/test_capacity", ("M20d", "M43a", "M43b", "M43c", "M43d", "M44", "M45")),
    ("tests/test_ci.py", ("M52",)),
    ("tests/test_claude", ("M35a", "M35b", "M35c")),
    ("tests/test_codex", ("M31a", "M32a", "M32b", "M32c", "M34a", "M34b", "M48")),
    ("tests/test_command_policy.py", ("M32b",)),
    ("tests/test_config.py", ("M13a", "M13b")),
    ("tests/test_context_hook.py", ("M22b", "M49")),
    ("tests/test_delivery", ("M46", "M47", "M48")),
    ("tests/test_dispatch.py", ("M12",)),
    ("tests/test_doc.py", ("M50b",)),
    ("tests/test_doctor.py", ("M50a",)),
    ("tests/test_domain.py", ("M11",)),
    ("tests/test_effective_policy.py", ("M15b",)),
    ("tests/test_launch", ("M06", "M28b", "M30c")),
    ("tests/test_lifecycle.py", ("M26", "M29", "M30b")),
    ("tests/test_logging_setup.py", ("M50a",)),
    ("tests/test_m008_integration.py", ("M54",)),
    ("tests/test_mcp.py", ("M10", "M42")),
    ("tests/test_native_settings.py", ("M13b", "M16")),
    ("tests/test_paths.py", ("M14a", "M23a")),
    ("tests/test_plugin_integration.py", ("M32c", "M35d")),
    ("tests/test_preparation.py", ("M28c",)),
    ("tests/test_priority_context_regressions.py", ("M22b", "M30d")),
    ("tests/test_process_identity.py", ("M07", "M26")),
    ("tests/test_profiles.py", ("M15b",)),
    ("tests/test_qwen_adapter.py", ("M37",)),
    ("tests/test_reconciliation.py", ("M30a",)),
    ("tests/test_release_script.py", ("M52", "M53")),
    ("tests/test_resume", ("M22", "M34b", "M38")),
    ("tests/test_role_plan.py", ("M15a", "M15b")),
    ("tests/test_run_stats.py", ("M45",)),
    ("tests/test_service.py", ("M19", "M30d")),
    ("tests/test_snapshots.py", ("M25",)),
    ("tests/test_state_db.py", ("M17", "M19")),
    ("tests/test_state_migrations.py", ("M17", "M18")),
    ("tests/test_state_outbox.py", ("M21", "M46", "M47")),
    ("tests/test_state_store.py", ("M17", "M19", "M20a", "M20b", "M21", "M22")),
    ("tests/test_supervisor", ("M28b", "M30b", "M30c")),
    ("tests/test_verify.py", ("M24", "M30b")),
    ("tests/test_wait.py", ("M30c",)),
)


def _test_tasks(path: str, valid_tasks: set[str]) -> list[str]:
    """Assign a Python test file to its board lane, with M02 as an audit fallback."""

    for prefix, task_ids in TEST_RULES:
        if path == prefix or path.startswith(prefix):
            if not set(task_ids) <= valid_tasks:
                raise ValueError(f"unknown test task IDs for {path}")
            return list(task_ids)
    return ["M02"]


def _test_names(root: Path) -> dict[str, list[tuple[str, str]]]:
    """Collect current Rust test functions keyed by their relative source path."""

    result: dict[str, list[tuple[str, str]]] = defaultdict(list)
    for path in sorted(root.glob("crates/**/tests/*.rs")):
        relative = path.relative_to(root).as_posix()
        for match in re.finditer(r"(?m)^\s*(?:pub\s+)?(?:async\s+)?fn\s+([a-zA-Z0-9_]+)\s*\(", path.read_text(encoding="utf-8")):
            result[relative].append((match.group(1), relative))
    return result


RUST_RULES: dict[str, tuple[tuple[str, str, str], ...]] = {
    "tests/test_answer_payload_proof.py": (
        ("inspect_legacy_requires", "crates/agent-run-core/tests/verification.rs", "legacy_frame_must_be_exactly_terminal_and_is_stripped_once"),
        ("legacy_embedded_sentinel", "crates/agent-run-core/tests/verification.rs", "legacy_frame_must_be_exactly_terminal_and_is_stripped_once"),
        ("legacy_terminal_frame", "crates/agent-run-core/tests/verification.rs", "legacy_frame_must_be_exactly_terminal_and_is_stripped_once"),
        ("malformed_or_contradicting_proof", "crates/agent-run-core/tests/verification.rs", "missing_sidecar_never_downgrades_to_legacy"),
        ("metadata_reads_are_bounded_and_reject_symlinks", "crates/agent-run-core/tests/verification.rs", "payload_and_parent_symlinks_are_not_followed"),
        ("metadata_symlink_swap", "crates/agent-run-core/tests/verification.rs", "payload_and_parent_symlinks_are_not_followed"),
        ("missing_proof", "crates/agent-run-core/tests/verification.rs", "missing_sidecar_never_downgrades_to_legacy"),
        ("read_answer_payload_legacy", "crates/agent-run-core/tests/verification.rs", "legacy_frame_must_be_exactly_terminal_and_is_stripped_once"),
        ("above_inline_limit", "crates/agent-run-core/tests/verification.rs", "non_inline_answer_is_still_verified"),
        ("invalid_utf8", "crates/agent-run-core/tests/verification.rs", "invalid_utf8_is_rejected_even_with_matching_legacy_hash"),
        ("legacy_descriptor", "crates/agent-run-core/tests/verification.rs", "legacy_frame_must_be_exactly_terminal_and_is_stripped_once"),
        ("missing_current_proof", "crates/agent-run-core/tests/verification.rs", "missing_sidecar_never_downgrades_to_legacy"),
        ("tampered_payload", "crates/agent-run-core/tests/verification.rs", "same_length_tampering_is_detected_by_hash"),
    ),
    "tests/test_api_socket.py": (
        ("ping_and_tools_discovery", "crates/agent-run/tests/protocol.rs", "ping_and_discovery_do_not_require_database_access"),
        ("unknown_method_and_validation_error", "crates/agent-run/tests/protocol.rs", "invalid_envelopes_and_unknown_method_get_standard_codes"),
        ("notifications_have_no_response", "crates/agent-run/tests/protocol.rs", "notifications_have_no_response_and_unknown_arguments_fail"),
        ("framing_rejects_partial_and_oversized", "crates/agent-run/tests/protocol.rs", "framing_rejects_partial_and_oversized_lines"),
    ),
    "tests/test_capacity_codex_appserver.py": (
        ("slice_freshness", "crates/agent-run-core/tests/capacity.rs", "freshness_rejects_future_expired_reset_and_unknown_evidence"),
        ("credit_metadata", "crates/agent-run-core/tests/capacity.rs", "codex_normalizer_keeps_windows_and_credit_metadata"),
    ),
    "tests/test_capacity_forecast.py": (
        ("future_observation", "crates/agent-run-core/tests/capacity.rs", "freshness_rejects_future_expired_reset_and_unknown_evidence"),
        ("past_reset_at", "crates/agent-run-core/tests/capacity.rs", "freshness_rejects_future_expired_reset_and_unknown_evidence"),
    ),
    "tests/test_capacity_identity.py": (
        ("base_label", "crates/agent-run-core/tests/capacity.rs", "nullable_account_identity_never_collides_with_a_label"),
        ("malformed_present_window", "crates/agent-run-core/tests/capacity.rs", "malformed_present_window_disables_the_whole_route"),
    ),
    "tests/test_capacity_outcomes.py": (
        ("invalid_slice_does_not_abort", "crates/agent-run-core/tests/capacity.rs", "invalid_atomic_slice_does_not_erase_a_committed_snapshot"),
    ),
    "tests/test_capacity_ranking.py": (
        ("exhaustion", "crates/agent-run-core/tests/capacity.rs", "exhausted_window_cannot_be_revived_by_weight"),
        ("alias_weight_is_maximum", "crates/agent-run-core/tests/capacity.rs", "aliases_use_highest_absolute_weight_not_sum"),
    ),
    "tests/test_capacity_reset_identity.py": (
        ("none_reset", "crates/agent-run-core/tests/capacity.rs", "reset_jitter_only_groups_still_open_windows"),
        ("past_reset_at", "crates/agent-run-core/tests/capacity.rs", "freshness_rejects_future_expired_reset_and_unknown_evidence"),
    ),
    "tests/test_capacity_topology.py": (
        ("invalid_pool_reference", "crates/agent-run-core/tests/capacity.rs", "every_route_pool_reference_is_validated"),
    ),
    "tests/test_config.py": (
        ("unknown_fields", "crates/agent-run-config/tests/domain_config.rs", "unknown_config_fields_fail_closed"),
        ("priority_account_and_lane_multipliers", "crates/agent-run-config/tests/domain_config.rs", "weights_are_absolute_with_account_precedence"),
    ),
    "tests/test_dispatch.py": (
        ("tools_table_is_exactly_pinned", "crates/agent-run/tests/protocol.rs", "packaged_table_has_exactly_the_shared_eleven_tools"),
    ),
    "tests/test_domain.py": (
        ("start_request_validates", "crates/agent-run-config/tests/domain_config.rs", "request_rejects_bad_timeout_and_duplicate_roots"),
        ("state_machine_matches", "crates/agent-run-config/tests/domain_config.rs", "transition_matrix_matches_contract"),
    ),
    "tests/test_native_settings.py": (
        ("validate_rejects_reserved_roots", "crates/agent-run-config/tests/domain_config.rs", "native_security_roots_cannot_be_overridden"),
    ),
    "tests/test_process_identity.py": (
        ("birth_is_observed_and_reuse", "crates/agent-run-platform/tests/process_identity.rs", "current_process_birth_is_observed_and_reuse_is_distinct"),
        ("missing_or_unproven_identity", "crates/agent-run-platform/tests/process_identity.rs", "missing_or_unproven_identity_is_not_automatically_death"),
    ),
    "tests/test_profiles.py": (
        ("canonical_role_owns", "crates/agent-run-config/tests/domain_config.rs", "canonical_role_owns_writes_and_retains_caller_constraints"),
        ("incomplete_or_unrevisioned", "crates/agent-run-config/tests/domain_config.rs", "incomplete_canonical_role_and_external_roots_fail"),
        ("named_profile_body_and_write", "crates/agent-run-config/tests/domain_config.rs", "legacy_profile_only_narrows_writes"),
        ("read_roots_are_resolved", "crates/agent-run-config/tests/domain_config.rs", "roots_form_a_minimal_antichain"),
    ),
    "tests/test_state_db.py": (
        ("invalid_and_newer_versions", "crates/agent-run-store/tests/state.rs", "newer_database_version_is_refused_without_upgrade"),
    ),
    "tests/test_state_migrations.py": (
        ("newer_schema_is_refused", "crates/agent-run-store/tests/state_migrations.rs", "newer_schema_is_refused_without_touching_the_store"),
    ),
    "tests/test_state_store.py": (
        ("same_request_id_is_distinct", "crates/agent-run-store/tests/state.rs", "request_ids_are_scoped_to_orchestrator"),
    ),
    "tests/test_verify.py": (
        ("legacy_frame", "crates/agent-run-core/tests/verification.rs", "legacy_frame_must_be_exactly_terminal_and_is_stripped_once"),
        ("invalid_utf8", "crates/agent-run-core/tests/verification.rs", "invalid_utf8_is_rejected_even_with_matching_legacy_hash"),
        ("symlink", "crates/agent-run-core/tests/verification.rs", "payload_and_parent_symlinks_are_not_followed"),
        ("traversal", "crates/agent-run-core/tests/verification.rs", "traversal_and_absolute_owned_paths_are_refused"),
    ),
}


def _rust_test_for(path: str, test_name: str, available: dict[str, list[tuple[str, str]]]) -> str | None:
    """Return a Rust test only for an explicit, behavior-specific equivalence rule."""

    rules = next((rules for prefix, rules in RUST_RULES.items() if path == prefix or path.startswith(prefix)), ())
    for needle, rust_path, rust_name in rules:
        if needle in test_name and rust_name in {name for name, _ in available.get(rust_path, [])}:
            return f"{rust_path}::{rust_name}"
    return None


def _test_records(paths: Sequence[str], tasks: dict[str, dict[str, str]], valid_tasks: set[str], available: dict[str, list[tuple[str, str]]]) -> list[dict[str, object]]:
    """Build one mapped record for every pytest collection ID in the baseline."""

    records: list[dict[str, object]] = []
    test_ids = (OUT / "test-ids.txt").read_text(encoding="utf-8").splitlines()
    for test_id in filter(None, test_ids):
        python_file, *parts = test_id.split("::")
        raw_name = parts[-1] if parts else python_file.rsplit("/", 1)[-1]
        test_name = raw_name.split("[", 1)[0]
        task_ids = _test_tasks(python_file, valid_tasks)
        rust_test = _rust_test_for(python_file, test_name, available)
        status = "ported" if rust_test else "planned" if task_ids else "unassigned"
        records.append(
            {
                "test_id": test_id,
                "python_file": python_file,
                "behavior": re.sub(r"\s+", " ", test_name.removeprefix("test_").replace("_", " ")).strip(),
                "plan_tests": ";".join(_plan_tests(task_ids, tasks)),
                "task_ids": ";".join(task_ids),
                "rust_test": rust_test or "",
                "status": status,
            }
        )
    return records


def _write_json(path: Path, value: object) -> None:
    """Write deterministic UTF-8 JSON with two-space indentation and a newline."""

    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _build_source_manifest(paths: Sequence[str]) -> list[dict[str, object]]:
    """Hash every baseline tree file and return sorted manifest records."""

    records = []
    for path in paths:
        content = _baseline_bytes(path)
        records.append(
            {
                "path": path,
                "sha256": hashlib.sha256(content).hexdigest(),
                "size": len(content),
                "kind": _kind(path),
            }
        )
    return records


def _build_inventory(paths: Sequence[str], valid_tasks: set[str]) -> list[dict[str, object]]:
    """Build source/resource/import ownership records for the Python package and scripts."""

    inventory_paths = [path for path in paths if path.startswith(("src/agent_run/", "scripts/"))]
    modules = {_module_name(path) for path in inventory_paths if _module_name(path)}
    records = []
    for path in inventory_paths:
        content = _baseline_bytes(path)
        task_ids, target = _source_owner(path, valid_tasks)
        records.append(
            {
                "path": path,
                "purpose": _purpose(path, content),
                "intra_package_imports": _intra_imports(path, content, modules),
                "packaged_resources": _resource_paths(path, content, paths),
                "rust_target": target,
                "task_ids": task_ids,
            }
        )
    return records


def _summary(manifest: Sequence[dict[str, object]], inventory: Sequence[dict[str, object]], tests: Sequence[dict[str, object]], tasks: dict[str, dict[str, str]]) -> str:
    """Render coverage counts, file/lane tables, and all unassigned ledger rows."""

    lines = [
        "# Python-to-Rust coverage summary",
        "",
        f"Generated from Python baseline `{PYTHON_BASELINE}`; no live engine evidence is implied.",
        "",
        "## Baseline files by kind",
        "",
        "| kind | count |",
        "| --- | ---: |",
    ]
    kinds = Counter(str(record["kind"]) for record in manifest)
    lines.extend(f"| {kind} | {kinds[kind]} |" for kind in sorted(kinds))
    statuses = Counter(str(record["status"]) for record in tests)
    lines.extend(
        [
            "",
            "## Test status",
            "",
            "| status | count |",
            "| --- | ---: |",
        ]
    )
    lines.extend(f"| {status} | {statuses[status]} |" for status in ("ported", "planned", "unassigned"))
    by_file: dict[str, Counter[str]] = defaultdict(Counter)
    for record in tests:
        by_file[str(record["python_file"])][str(record["status"])] += 1
    lines.extend(["", "## Test coverage by Python file", "", "| Python file | total | ported | planned | unassigned |", "| --- | ---: | ---: | ---: | ---: |"])
    for path in sorted(by_file):
        counts = by_file[path]
        lines.append(f"| {path} | {sum(counts.values())} | {counts['ported']} | {counts['planned']} | {counts['unassigned']} |")
    by_lane: dict[str, Counter[str]] = defaultdict(Counter)
    for record in tests:
        for task_id in filter(None, str(record["task_ids"]).split(";")):
            by_lane[tasks[task_id]["lane"]][str(record["status"])] += 1
    lines.extend(["", "## Test coverage by lane", "", "| lane | status | rows |", "| --- | --- | ---: |"])
    for lane in sorted(by_lane):
        for status in ("ported", "planned", "unassigned"):
            lines.append(f"| {lane} | {status} | {by_lane[lane][status]} |")
    unassigned_files = [str(record["path"]) for record in inventory if record["rust_target"] == "unassigned"]
    unassigned_tests = [str(record["test_id"]) for record in tests if record["status"] == "unassigned"]
    lines.extend(["", "## Unassigned files and tests", ""])
    lines.append("- Files: " + (", ".join(unassigned_files) if unassigned_files else "none."))
    lines.append("- Tests: " + (", ".join(unassigned_tests) if unassigned_tests else "none."))
    lines.extend(["", "## Proposed board additions", "", "No additions proposed; every baseline source/test row has an owner task."])
    return "\n".join(lines) + "\n"


def _write_csv(path: Path, records: Sequence[dict[str, object]]) -> None:
    """Write the exact test-map column order required by the migration gate."""

    fields = ["test_id", "python_file", "behavior", "plan_tests", "task_ids", "rust_test", "status"]
    with path.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields, lineterminator="\n")
        writer.writeheader()
        writer.writerows({field: record[field] for field in fields} for record in records)


def build() -> None:
    """Generate all P0 manifests and the human-readable coverage summary."""

    paths = _baseline_paths()
    task_rows, tasks = _load_tasks()
    valid_tasks = {row["id"] for row in task_rows}
    manifest = _build_source_manifest(paths)
    inventory = _build_inventory(paths, valid_tasks)
    available = _test_names(ROOT)
    tests = _test_records(paths, tasks, valid_tasks, available)
    OUT.mkdir(parents=True, exist_ok=True)
    _write_json(OUT / "source-manifest.json", manifest)
    _write_json(OUT / "inventory.json", inventory)
    _write_csv(OUT / "test-map.csv", tests)
    (OUT / "coverage-summary.md").write_text(_summary(manifest, inventory, tests, tasks), encoding="utf-8")
    print(f"wrote {len(manifest)} files, {len(inventory)} inventory rows, {len(tests)} test rows")


def main(argv: Sequence[str] | None = None) -> int:
    """Parse the development-tool CLI and return a process exit status."""

    parser = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    parser.parse_args(argv)
    build()
    return 0


if __name__ == "__main__":
    sys.exit(main())
