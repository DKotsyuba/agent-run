# P12 review checklist — for the owner to run and sign

Status: **unsigned draft.** Nothing here is ticked. The plan (§18.12) requires a
signed review checklist as an exit condition for P12, and no such artifact
existed; this is the form, not the result.

**Do not let the agent that performed the migration fill this in.** Every line
below is either a command you run yourself and read the output of, or a decision
only you can make. A checklist completed by the implementer is not an
acceptance.

The plan is explicit about what does *not* close P12: a document, a crate
scaffold, or `cargo test` against mocks. It is equally explicit that a README
disclaimer does not close a risk — the release scope you claim must match the
actual result.

---

## 0. Before you start

```sh
cd <the worktree you are accepting>
export CARGO_HOME=<a cargo home with the offline registry>
git log --oneline -1          # record this SHA on the signature line
git status --porcelain        # must be empty apart from untracked scratch
```

Record the commit you are accepting. Everything below refers to that exact
revision, not to "the branch".

---

## 1. Commands that must pass, run by you

| # | Command | What a pass looks like | Result |
|---|---|---|---|
| 1.1 | `cargo fmt --all -- --check` | exit 0, no output | |
| 1.2 | `cargo clippy --offline --workspace --all-targets --all-features` | exit 0, **zero** warnings | |
| 1.3 | `cargo test --offline --workspace --all-features --no-fail-fast` | 0 failed. In a sandbox that forbids Unix sockets you will instead see ~56 failures across 7 targets, every one a bind denial — verify that by reading a panic message, not by counting | |
| 1.4 | `cargo xtask qualify --release` | exit 0; reports the host platform as evidenced and names the pending live portions **without claiming them** | |
| 1.5 | `cargo xtask archive --verify` | exit 0, names the archive it verified | |
| 1.6 | `cargo xtask evidence verify` | exit 0, entry count matches `migration/evidence/index.json` | |
| 1.7 | `cargo test --offline --workspace --all-features --locked` | same as 1.3; proves the committed lockfile resolves | |

**Read `$?` from the command itself.** A pipeline into `head`, `tail` or `grep`
reports the exit status of the last stage, which will cheerfully report success
for a failed build. This mistake was made repeatedly during the migration and
each time it looked like a broken product.

If auditing the lockfile offline, filter by platform:
`cargo metadata --locked --offline --filter-platform aarch64-apple-darwin`.
The unfiltered form fails on an Android-only transitive dependency; that is an
artifact of cross-platform resolution, not a stale lock.

---

## 2. Decisions only you can make

None of these is closed by work. Each blocks something specific.

| # | Decision | What it blocks | Where the evidence is |
|---|---|---|---|
| 2.1 | **Process-group kill strategy.** Python kills the recorded group unconditionally; this port signals only a leader it has verified alive. | The single unported behavior, and part of risk K02 | `migration/adr/A10-spawn-backend.md`, section "Measured cost of decision 5" |
| 2.2 | **Supported platforms.** Every run to date was macOS/arm64. Linux code exists and has never executed. | Risk K02's "Mac/Linux" closure condition; what the release may claim | `migration/adr/A15-platforms.md` (Proposed) |
| 2.3 | **Desktop relay boundary.** Direct Rust admission is refused by the host; option B runs a minimal shim through the Desktop-supplied Node. | **Blocking risk K01**, board row M09 | `migration/adr/A09-desktop-relay.md` (Proposed) |
| 2.4 | **Live qualification authorization.** Three scenarios need real-world resources: an installed `qwen` binary, the running ChatGPT Desktop host, one non-macOS machine. | Board row M55; the `live` portions of T57, T71, T82 | `migration/evidence/qualification-scope.md` |
| 2.5 | **Commit signing before push.** The signing agent was unreachable in the sandbox, so commits were made with signing disabled. | Any push | — |

For 2.3 specifically: a signed Apple certificate of our own does **not** solve
it. The host authenticates its peer against a specific team identity, so the
question is whose signature, not whether one exists.

---

## 3. Risks the plan marks blocking

"Blocking" means the full native release cannot be accepted without closing it.

| Risk | Closure condition (from the plan) | Verify by |
|---|---|---|
| K01 Desktop capability without a signed shim | T71 on the real host | reading `qualification-scope.md` row T71 — it is `partial`, live pending |
| K02 PID reuse / birth time → signalling a foreign process | T30–T33 on Mac **and Linux** | rows T30–T33; note Linux has never run |
| K03 fork/pre_exec deadlock in a threaded broker | T25–T28 | rows T25–T28 |
| K04 transactions cause double launch/resume/delivery | T18–T23 | rows T18–T23 |
| K05 canonical JSON mismatch breaks resume | T46, T58 | rows T46, T58 |
| K06 path/symlink race reads a secret or swaps a proof | T42–T44 | rows T42–T44 |
| K07 exit=0 / EOF / sentinel text wrongly becomes success | T29, T37–T44, T52 | those rows |

---

## 4. What the board's statuses do and do not mean

The plan distinguishes `implemented` from `verified` and requires an explicit
flag when live verification is absent, rather than a green status that hides it.

At the time this form was written, `migration/tasks.csv` carried **no row marked
`verified`**. A count of implemented rows is therefore not a readiness figure.
Check this yourself:

```sh
python3 -c "
import csv, collections
rows = list(csv.DictReader(open('migration/tasks.csv')))
print(collections.Counter(r['status'] for r in rows))
print([r['id'] for r in rows if r['status'] != 'implemented'])
"
```

---

## 5. Declared divergences — read before accepting parity claims

The port does not reproduce every Python behavior, by decision rather than
omission. The ledger is `migration/baseline/divergences.csv`, one row per
behavior, each naming the ADR that decided it. Confirm the count and the
reasons match what you are willing to ship:

```sh
awk -F, 'NR>1 {print $2}' migration/baseline/divergences.csv | sort | uniq -c
```

The largest group is release and deploy (A19): GitHub publication, pip/venv
install, release-time smoke, launchd control, and mid-deploy schema migration.
A practical consequence for the cutover: **steps C5–C7 of the runbook — restore
services, wait for API readiness, release smoke — are not implemented by
`cargo xtask` on purpose.** The tool owns the journal and the pointer swap. The
rest of the runbook is the operator's, or the Python release script's.

---

## 6. Before any production cutover

Per §21, in this order:

1. Run the §21 runbook on a **copy** of the environment, not production.
2. Complete the §21.6 recovery drills on a disposable home: interrupt after each
   phase, and separately exercise corrupt candidate, missing assets, DB busy,
   insufficient disk, permission denied, failed readiness, lost auth/history
   path. The result must be a journal naming the next operation, not "try again".
3. Only then, and only with your separate authorization, touch a production home.

The port refuses to resume Python-created runs (ADR A18). Cutover therefore
requires a quiescent installation with no unfinished Python-created runs.

---

## 7. Signature

I have run the commands in section 1 myself and read their output; I have made
the decisions in section 2; I accept the divergences in section 5.

```
Commit accepted: ______________________________________
Platforms claimed: ____________________________________
Live qualification: [ ] authorized and run  [ ] deferred, and not claimed
Name / date: __________________________________________
```

Leaving section 2 unanswered is itself a decision: it means the release is not
accepted, not that the questions were unimportant.
