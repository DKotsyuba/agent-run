#!/usr/bin/env python3
"""Deterministically regenerate migration/baseline/test-map.csv (rust_test,
status columns) and migration/evidence/coverage-<date>.md from:

  - the current Rust workspace's `cargo test -- --list` enumeration, and
  - the `Mirrors Python \\`...\\`` doc comments Rust test authors leave above
    each #[test], plus a legacy-match seed carried over from a prior run of
    this same script (so a manually-verified match that has no machine
    readable comment survives future regenerations as long as the Rust test
    it names still exists).

Dev-only measurement tool. Does not touch anything under crates/, sql/,
assets/ or xtask/ -- it only reads them and `cargo test --list`s them.

Usage:
    <python3.14> migration/tools/coverage_report.py [--date YYYY-MM-DD] [--check]

--check runs the same computation but does not write files; exit 1 if the
on-disk test-map.csv or coverage report would differ (useful in CI).
"""
from __future__ import annotations

import argparse
import csv
import dataclasses
import datetime as dt
import os
import re
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
TEST_MAP_CSV = REPO_ROOT / "migration/baseline/test-map.csv"
TASKS_CSV = REPO_ROOT / "migration/tasks.csv"
EVIDENCE_DIR = REPO_ROOT / "migration/evidence"

# Workspace member package name -> crate directory (Cargo.toml [workspace]).
PKG_TO_DIR = {
    "agent_run": "agent-run",
    "agent_run_adapters": "agent-run-adapters",
    "agent_run_config": "agent-run-config",
    "agent_run_core": "agent-run-core",
    "agent_run_domain": "agent-run-domain",
    "agent_run_platform": "agent-run-platform",
    "agent_run_store": "agent-run-store",
    "xtask": "xtask",
}

# lane (tasks.csv) -> display label used in the per-area table.
LANE_LABELS = {
    "store": "store",
    "platform/artifacts": "platform/artifacts",
    "config/roles": "config/roles",
    "core lifecycle": "core lifecycle",
    "codex adapter": "Codex adapter",
    "claude/glm/qwen adapters": "Claude/GLM/Qwen",
    "transports/CLI": "transports/CLI",
    "capacity": "capacity",
    "delivery/hooks": "delivery/hooks",
    "operations/release": "operations/release",
    "baseline/control": "baseline/control",
}
AREA_ORDER = [
    "store",
    "platform/artifacts",
    "config/roles",
    "core lifecycle",
    "codex adapter",
    "claude/glm/qwen adapters",
    "transports/CLI",
    "capacity",
    "delivery/hooks",
    "operations/release",
    "baseline/control",
]

TEST_MAP_FIELDS = [
    "test_id",
    "python_file",
    "behavior",
    "plan_tests",
    "task_ids",
    "rust_test",
    "status",
]

BINARY_HEADER_RE = re.compile(
    r"^\s*Running (?:unittests )?(?P<path>\S+) \((?:\S*/)?target/[^()]*/(?P<pkg>.+)-[0-9a-f]{16}\)\s*$"
)
TEST_LINE_RE = re.compile(r"^(?P<name>[A-Za-z0-9_:<>]+): test$")
COUNT_LINE_RE = re.compile(r"^(\d+) tests?, \d+ benchmarks?")
MIRROR_RE = re.compile(r"[Mm]irrors(?:\s+Python)?[^`\n]*`([^`]+)`")


@dataclasses.dataclass
class PyTest:
    test_id: str
    python_file: str
    behavior: str
    plan_tests: str
    task_ids: str
    legacy_rust_test: str
    legacy_status: str


@dataclasses.dataclass
class RustTest:
    source_file: str  # repo-relative, e.g. crates/agent-run-core/tests/verification.rs
    full_name: str  # as printed by `cargo test --list`, may include module path
    fn_name: str  # last :: segment

    @property
    def rust_test_id(self) -> str:
        return f"{self.source_file}::{self.full_name}"


def run_cargo_list() -> str:
    cargo_home = os.environ.get("CARGO_HOME")
    if not cargo_home:
        print(
            "warning: CARGO_HOME is not set in the environment; "
            "cargo will use its default (likely wrong for this worktree).",
            file=sys.stderr,
        )
    # cargo prints its own `Running <binary>` progress lines to stderr while
    # the test harness's `--list` output goes to stdout; merge them (like a
    # shell `2>&1`) so the two interleave in the order we need to parse them.
    proc = subprocess.run(
        ["cargo", "test", "--offline", "--workspace", "--all-features", "--", "--list"],
        cwd=REPO_ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        check=False,
    )
    if proc.returncode != 0:
        raise SystemExit(f"cargo test --list failed (exit {proc.returncode}):\n{proc.stdout}")
    return proc.stdout


def resolve_unit_test_source(pkg_dir: str, reported_path: str, full_name: str) -> str:
    """`reported_path` is what `cargo test --list` prints after `Running
    unittests` (e.g. `src/lib.rs`). For a `mod tests { #[test] fn foo() }`
    buried in a submodule, the real source file is not src/lib.rs -- it is
    found by walking `full_name`'s module path (stripping the trailing
    `tests::<fn>` segment) onto the crate's src/ tree.
    """
    parts = full_name.split("::")
    mod_parts = parts[:-1]  # drop the fn name
    if mod_parts and mod_parts[-1] == "tests":
        mod_parts = mod_parts[:-1]
    if not mod_parts:
        return f"crates/{pkg_dir}/{reported_path}" if pkg_dir != "xtask" else f"xtask/{reported_path}"
    base = REPO_ROOT / ("crates" / Path(pkg_dir) if pkg_dir != "xtask" else Path("xtask")) / "src"
    candidates = [
        base.joinpath(*mod_parts).with_suffix(".rs"),
        base.joinpath(*mod_parts, "mod.rs"),
    ]
    for cand in candidates:
        if cand.is_file():
            return str(cand.relative_to(REPO_ROOT))
    # Fall back to the reported path; matching will simply miss comments.
    return f"crates/{pkg_dir}/{reported_path}" if pkg_dir != "xtask" else f"xtask/{reported_path}"


def parse_cargo_list(output: str) -> list[RustTest]:
    tests: list[RustTest] = []
    cur_crate_pkg: str | None = None  # updated only by the crate's own src/lib.rs|main.rs target
    cur_pkg: str | None = None
    cur_path: str | None = None
    cur_is_unit = False
    for line in output.splitlines():
        m = BINARY_HEADER_RE.match(line)
        if m:
            cur_path = m.group("path")
            cur_pkg = m.group("pkg")
            cur_is_unit = "unittests" in line
            if cur_is_unit and cur_path in ("src/lib.rs", "src/main.rs"):
                # cargo names integration-test and other auxiliary binaries after
                # the test *file*, not the owning package, so the owning crate's
                # package name can only be learned from its own lib/main target;
                # every following `Running` line (until the next lib/main target)
                # belongs to this same crate.
                cur_crate_pkg = cur_pkg
            continue
        if COUNT_LINE_RE.match(line):
            continue
        m = TEST_LINE_RE.match(line)
        if not m or cur_path is None:
            continue
        full_name = m.group("name")
        fn_name = full_name.rsplit("::", 1)[-1]
        pkg_dir = PKG_TO_DIR.get(cur_crate_pkg or "", cur_crate_pkg or cur_pkg or "unknown")
        if cur_is_unit:
            source_file = resolve_unit_test_source(pkg_dir, cur_path, full_name)
        else:
            source_file = (
                f"crates/{pkg_dir}/{cur_path}" if pkg_dir != "xtask" else f"xtask/{cur_path}"
            )
        tests.append(RustTest(source_file=source_file, full_name=full_name, fn_name=fn_name))
    return tests


def load_python_tests(seed_ref: str = "") -> list[PyTest]:
    """`seed_ref`, if given, is a git ref (e.g. `HEAD`) to read the prior
    test_id/rust_test/status columns from instead of the working tree copy --
    use this so re-running the tool while developing it does not fold its own
    previous (possibly buggy) output back in as next run's "legacy seed"."""
    out = []
    if seed_ref:
        text = subprocess.run(
            ["git", "show", f"{seed_ref}:migration/baseline/test-map.csv"],
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            check=True,
        ).stdout
        reader = csv.DictReader(text.splitlines())
    else:
        reader = csv.DictReader(TEST_MAP_CSV.open(newline=""))
    for row in reader:
        out.append(
            PyTest(
                test_id=row["test_id"],
                python_file=row["python_file"],
                behavior=row["behavior"],
                plan_tests=row["plan_tests"],
                task_ids=row["task_ids"],
                legacy_rust_test=row.get("rust_test", ""),
                legacy_status=row.get("status", ""),
            )
        )
    return out


def load_tasks() -> dict[str, dict[str, str]]:
    out = {}
    with TASKS_CSV.open(newline="") as fh:
        for row in csv.DictReader(fh):
            out[row["id"]] = row
    return out


def extract_comment_block(lines: list[str], fn_line_idx: int) -> list[str]:
    """Walk upward from the `fn NAME(` line, skipping attribute lines
    (`#[...]`), collecting a contiguous run of `//`/`///` comment lines.
    Stops at the first blank or unrelated line.
    """
    i = fn_line_idx - 1
    # Skip stacked attributes directly above the fn (e.g. #[test], #[should_panic]).
    while i >= 0 and lines[i].strip().startswith("#["):
        i -= 1
    block = []
    while i >= 0:
        stripped = lines[i].strip()
        if stripped.startswith("///") or stripped.startswith("//"):
            block.append(stripped.lstrip("/").strip())
            i -= 1
        else:
            break
    block.reverse()
    return block


FN_RE_CACHE: dict[str, list[str]] = {}


def find_fn_line(lines: list[str], fn_name: str) -> int | None:
    pattern = re.compile(r"^\s*(?:pub(?:\([^)]*\))?\s+)?(?:async\s+)?fn\s+" + re.escape(fn_name) + r"\s*[(<]")
    for idx, line in enumerate(lines):
        if pattern.match(line):
            return idx
    return None


def looks_test_like(ref: str) -> bool:
    tail = re.split(r"::|\.", ref)[-1]
    return tail.startswith("test_")


def resolve_mirror_ref(
    ref: str, by_suffix: dict[str, list[PyTest]], by_file_suffix: dict[tuple[str, str], list[PyTest]]
) -> PyTest | None:
    ref = ref.strip().strip(".")
    if not looks_test_like(ref):
        return None
    if "::" in ref:
        parts = ref.split("::")
        if parts[0].endswith(".py"):
            pyfile = parts[0] if parts[0].startswith("tests/") else f"tests/{parts[0]}"
            method = parts[-1]
            cands = by_file_suffix.get((pyfile, method), [])
            if len(cands) == 1:
                return cands[0]
            if not cands:
                cands = by_suffix.get(method, [])
                if len(cands) == 1:
                    return cands[0]
            return None
        # Class::method with no file, or bare method after stripping empties.
        method = parts[-1]
        cands = by_suffix.get(method, [])
        return cands[0] if len(cands) == 1 else None
    if "." in ref:
        method = ref.rsplit(".", 1)[-1]
        cands = by_suffix.get(method, [])
        return cands[0] if len(cands) == 1 else None
    # bare method name
    cands = by_suffix.get(ref, [])
    return cands[0] if len(cands) == 1 else None


def build_indices(py_tests: list[PyTest]):
    by_suffix: dict[str, list[PyTest]] = {}
    by_file_suffix: dict[tuple[str, str], list[PyTest]] = {}
    for t in py_tests:
        short = t.test_id.rsplit("::", 1)[-1]
        by_suffix.setdefault(short, []).append(t)
        by_file_suffix.setdefault((t.python_file, short), []).append(t)
    return by_suffix, by_file_suffix


def match_rust_tests(
    rust_tests: list[RustTest], py_tests: list[PyTest]
) -> tuple[dict[str, str], list[str]]:
    """Returns (python_test_id -> rust_test_id, warnings)."""
    by_suffix, by_file_suffix = build_indices(py_tests)
    assigned: dict[str, str] = {}
    warnings: list[str] = []
    source_cache: dict[str, list[str]] = {}

    for rt in rust_tests:
        src_path = REPO_ROOT / rt.source_file
        if not src_path.is_file():
            continue
        if rt.source_file not in source_cache:
            source_cache[rt.source_file] = src_path.read_text(errors="replace").splitlines()
        lines = source_cache[rt.source_file]
        fn_idx = find_fn_line(lines, rt.fn_name)
        if fn_idx is None:
            continue
        block = extract_comment_block(lines, fn_idx)
        if not block:
            continue
        joined = " ".join(block)
        refs = MIRROR_RE.findall(joined)
        for ref in refs:
            py = resolve_mirror_ref(ref, by_suffix, by_file_suffix)
            if py is None:
                continue
            if py.test_id in assigned and assigned[py.test_id] != rt.rust_test_id:
                warnings.append(
                    f"conflict: {py.test_id} already -> {assigned[py.test_id]}, "
                    f"also claimed by {rt.rust_test_id} (kept first)"
                )
                continue
            assigned[py.test_id] = rt.rust_test_id

    return assigned, warnings


def apply_legacy_seed(
    py_tests: list[PyTest], assigned: dict[str, str], rust_ids: set[str]
) -> list[str]:
    """A prior run's `ported` rows whose rust_test string still names a test
    `cargo test --list` reports today are kept even if this run's comment
    scan did not (re-)discover them -- e.g. Rust test files with no `Mirrors`
    doc comment at all, matched by hand in an earlier pass."""
    notes = []
    for t in py_tests:
        if t.test_id in assigned:
            continue
        if t.legacy_status == "ported" and t.legacy_rust_test:
            if t.legacy_rust_test in rust_ids:
                assigned[t.test_id] = t.legacy_rust_test
            else:
                notes.append(
                    f"dropped stale legacy match: {t.test_id} -> {t.legacy_rust_test} "
                    "(no longer in `cargo test --list`)"
                )
    return notes


def compute_status(t: PyTest, assigned: dict[str, str]) -> tuple[str, str]:
    rust_test = assigned.get(t.test_id, "")
    if rust_test:
        return rust_test, "ported"
    if t.task_ids.strip():
        return "", "planned"
    return "", "unassigned"


def primary_lane(task_ids: str, tasks: dict[str, dict[str, str]]) -> str | None:
    for tid in task_ids.split(";"):
        tid = tid.strip()
        if tid in tasks:
            return tasks[tid]["lane"]
    return None


def write_test_map(py_tests: list[PyTest], assigned: dict[str, str]) -> list[dict[str, str]]:
    rows = []
    for t in py_tests:
        rust_test, status = compute_status(t, assigned)
        rows.append(
            {
                "test_id": t.test_id,
                "python_file": t.python_file,
                "behavior": t.behavior,
                "plan_tests": t.plan_tests,
                "task_ids": t.task_ids,
                "rust_test": rust_test,
                "status": status,
            }
        )
    return rows


def render_report(
    date: str,
    base_sha: str,
    rows: list[dict[str, str]],
    tasks: dict[str, dict[str, str]],
    before_counts: dict[str, int],
    warnings: list[str],
    legacy_notes: list[str],
    rust_test_total: int,
) -> str:
    after_counts: dict[str, int] = {"ported": 0, "planned": 0, "unassigned": 0}
    for r in rows:
        after_counts[r["status"]] += 1

    area_rows: dict[str, dict[str, int]] = {a: {"ported": 0, "uncovered": 0} for a in AREA_ORDER}
    unowned: list[dict[str, str]] = []
    for r in rows:
        lane = primary_lane(r["task_ids"], tasks)
        if lane is None:
            unowned.append(r)
            continue
        bucket = area_rows.setdefault(lane, {"ported": 0, "uncovered": 0})
        if r["status"] == "ported":
            bucket["ported"] += 1
        else:
            bucket["uncovered"] += 1

    # Largest uncovered clusters: group uncovered rows by python_file, sort by size.
    from collections import Counter, defaultdict

    file_uncovered = Counter()
    file_task = {}
    file_owner_tasks: dict[str, set[str]] = defaultdict(set)
    for r in rows:
        if r["status"] == "ported":
            continue
        file_uncovered[r["python_file"]] += 1
        for tid in r["task_ids"].split(";"):
            tid = tid.strip()
            if tid:
                file_owner_tasks[r["python_file"]].add(tid)

    top_clusters = file_uncovered.most_common(8)

    # Board cross-check.
    task_test_status: dict[str, list[str]] = defaultdict(list)
    for r in rows:
        for tid in r["task_ids"].split(";"):
            tid = tid.strip()
            if tid:
                task_test_status[tid].append(r["status"])

    implemented_but_uncovered = []
    planned_but_covered = []
    for tid, task in sorted(tasks.items()):
        statuses = task_test_status.get(tid, [])
        if not statuses:
            continue
        n_uncovered = sum(1 for s in statuses if s != "ported")
        n_ported = sum(1 for s in statuses if s == "ported")
        if task["status"] == "implemented" and n_uncovered > 0:
            implemented_but_uncovered.append((tid, task["title"], n_uncovered, len(statuses)))
        if task["status"] == "planned" and n_ported > 0 and n_ported == len(statuses):
            planned_but_covered.append((tid, task["title"], n_ported))

    lines = []
    lines.append(f"# Rust coverage of the Python behavior baseline -- {date}")
    lines.append("")
    lines.append(f"Board row: M54. Measured against `{base_sha}` in `.wt/coverage`.")
    lines.append(
        f"Rust suite: `cargo test --offline --workspace --all-features -- --list` "
        f"enumerates {rust_test_total} tests."
    )
    lines.append("")
    lines.append("## Totals")
    lines.append("")
    lines.append("| status | before | after |")
    lines.append("| --- | ---: | ---: |")
    for s in ("ported", "planned", "unassigned"):
        lines.append(f"| {s} | {before_counts.get(s, 0)} | {after_counts.get(s, 0)} |")
    lines.append(f"| **total** | **{sum(before_counts.values())}** | **{len(rows)}** |")
    lines.append("")
    lines.append("## Per-area coverage")
    lines.append("")
    lines.append("| area | covered (ported) | uncovered (planned+unassigned) | total |")
    lines.append("| --- | ---: | ---: | ---: |")
    for area in AREA_ORDER:
        b = area_rows.get(area, {"ported": 0, "uncovered": 0})
        total = b["ported"] + b["uncovered"]
        if total == 0:
            continue
        lines.append(f"| {LANE_LABELS.get(area, area)} | {b['ported']} | {b['uncovered']} | {total} |")
    if unowned:
        lines.append(f"| *(no owning task)* | 0 | {len(unowned)} | {len(unowned)} |")
    lines.append("")
    lines.append("## Largest uncovered behavior clusters")
    lines.append("")
    lines.append("| python file | uncovered tests | owning task(s) |")
    lines.append("| --- | ---: | --- |")
    for pyfile, count in top_clusters:
        owners = ";".join(sorted(file_owner_tasks.get(pyfile, []))) or "*(none)*"
        lines.append(f"| {pyfile} | {count} | {owners} |")
    lines.append("")
    lines.append("## Python behaviors with no owning task at all")
    lines.append("")
    if unowned:
        lines.append("| test_id | python_file |")
        lines.append("| --- | --- |")
        for r in unowned[:50]:
            lines.append(f"| {r['test_id']} | {r['python_file']} |")
        if len(unowned) > 50:
            lines.append(f"| ... | {len(unowned) - 50} more |")
    else:
        lines.append("None -- every baseline test row carries at least one task_id.")
    lines.append("")
    lines.append("## Board cross-check")
    lines.append("")
    lines.append("### Tasks marked `implemented` with still-uncovered Python tests")
    lines.append("")
    if implemented_but_uncovered:
        lines.append("| task | title | uncovered / total |")
        lines.append("| --- | --- | ---: |")
        for tid, title, unc, tot in sorted(implemented_but_uncovered, key=lambda x: -x[2]):
            lines.append(f"| {tid} | {title} | {unc} / {tot} |")
    else:
        lines.append("None.")
    lines.append("")
    lines.append("### Tasks marked `planned` whose Python tests are already fully covered")
    lines.append("")
    if planned_but_covered:
        lines.append("| task | title | ported tests |")
        lines.append("| --- | --- | ---: |")
        for tid, title, n in planned_but_covered:
            lines.append(f"| {tid} | {title} | {n} |")
    else:
        lines.append("None.")
    lines.append("")
    if warnings or legacy_notes:
        lines.append("## Matcher notes")
        lines.append("")
        for w in warnings:
            lines.append(f"- {w}")
        for n in legacy_notes:
            lines.append(f"- {n}")
        lines.append("")
    return "\n".join(lines) + "\n"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=dt.date.today().isoformat())
    ap.add_argument("--base-sha", default="")
    ap.add_argument("--check", action="store_true")
    ap.add_argument(
        "--seed-ref",
        default="",
        help="git ref to read the prior rust_test/status seed from instead of "
        "the working tree (e.g. HEAD); use when the working tree copy is "
        "itself scratch output from an earlier run of this tool.",
    )
    args = ap.parse_args()

    py_tests = load_python_tests(args.seed_ref)
    before_counts: dict[str, int] = {"ported": 0, "planned": 0, "unassigned": 0}
    for t in py_tests:
        before_counts[t.legacy_status] = before_counts.get(t.legacy_status, 0) + 1

    tasks = load_tasks()
    cargo_out = run_cargo_list()
    rust_tests = parse_cargo_list(cargo_out)
    rust_ids = {rt.rust_test_id for rt in rust_tests}

    assigned, warnings = match_rust_tests(rust_tests, py_tests)
    legacy_notes = apply_legacy_seed(py_tests, assigned, rust_ids)

    rows = write_test_map(py_tests, assigned)

    report = render_report(
        date=args.date,
        base_sha=args.base_sha or "(unspecified)",
        rows=rows,
        tasks=tasks,
        before_counts=before_counts,
        warnings=warnings,
        legacy_notes=legacy_notes,
        rust_test_total=len(rust_tests),
    )

    report_path = EVIDENCE_DIR / f"coverage-{args.date}.md"

    if args.check:
        cur = TEST_MAP_CSV.read_text().replace("\r\n", "\n")
        new_csv_text = render_csv(rows).replace("\r\n", "\n")
        ok = cur == new_csv_text
        if not ok:
            print("test-map.csv would change", file=sys.stderr)
        if report_path.is_file():
            same_report = report_path.read_text() == report
            ok = ok and same_report
            if not same_report:
                print(f"{report_path.name} would change", file=sys.stderr)
        print("OK" if ok else "DIFFERS")
        return 0 if ok else 1

    with TEST_MAP_CSV.open("w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=TEST_MAP_FIELDS)
        writer.writeheader()
        writer.writerows(rows)

    EVIDENCE_DIR.mkdir(parents=True, exist_ok=True)
    report_path.write_text(report)

    print(f"wrote {TEST_MAP_CSV.relative_to(REPO_ROOT)} ({len(rows)} rows)")
    print(f"wrote {report_path.relative_to(REPO_ROOT)}")
    print(f"before: {before_counts}")
    after_counts = {"ported": 0, "planned": 0, "unassigned": 0}
    for r in rows:
        after_counts[r["status"]] += 1
    print(f"after:  {after_counts}")
    if warnings:
        print(f"{len(warnings)} matcher conflict warning(s), see report", file=sys.stderr)
    if legacy_notes:
        print(f"{len(legacy_notes)} stale legacy match(es) dropped, see report", file=sys.stderr)
    return 0


def render_csv(rows: list[dict[str, str]]) -> str:
    import io

    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=TEST_MAP_FIELDS)
    writer.writeheader()
    writer.writerows(rows)
    return buf.getvalue()


if __name__ == "__main__":
    raise SystemExit(main())
