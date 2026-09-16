# Defects the port found in itself

These are correctness defects discovered in the Rust implementation while
closing the qualification gaps, not defects in the Python baseline. Each was
found by a test or a run, never by reading a report. Each is fixed and merged,
and each now has a test that fails if it returns.

They are collected here because they share a shape worth knowing before
reviewing or extending this code.

## The pattern: four of these are the same defect

Four of the seven are the same failure in different places — **Python names a
deadline or refuses to answer, and the Rust port waits forever or answers
"clean"**:

| Site | Python's rule | What Rust did |
|---|---|---|
| `crates/agent-run-platform/src/process.rs` | returns `None` (unknown) when the leader is not alive — `src/agent_run/lifecycle.py:134-155`, `:279-296` | reported `Some(true)` — "all descendants gone" — from an observation it could not make |
| `crates/agent-run-adapters/src/io.rs:287` | joins each reader with a 1 s bound, then force-closes the pipes — `src/agent_run/adapters/codex/process_transport.py:276-294` | bounded the child wait at 3 s, then drained reader tasks with no bound at all |
| `crates/agent-run/src/transport/socket.rs` | finite server wait (60 s), finite client read (65 s), retry the same durable agent — `src/agent_run/cli.py:452-461`, `:56-57` | read the response frame with no deadline while every other method had 120 s |
| `crates/agent-run-platform/src/keychain.rs` | one 5 s timeout over the whole call — `src/agent_run/doctor.py:419-424`, `:42` | bounded the exit wait only; the output drain after it was unbounded |

A fifth, `crates/agent-run-core/src/doctor.rs`, is the same family: Python bounds
each `ps` at 2 s and degrades to an empty map (`doctor.py:601-605`, `:524`); Rust
called `Command::output()` with no timeout — in exactly the loaded-host
condition where `ps` stalls and `doctor` is most needed.

**If you extend this port, look for more of these.** Search for a wait on
something a peer controls — a `JoinHandle`, a channel `recv`, a pipe read, a
child `wait`, a lock — that no `tokio::time::timeout` covers. A visible timeout
earlier in the same function does not make the sequence bounded: that is how the
`io.rs` defect hid.

## Two comments asserted a guarantee the code did not provide

Worse than an absent bound is a stated one that is false, because it survives
review:

- `socket.rs` carried `// wait deliberately has no client-owned execution
  deadline.` It reads as a decision. No ADR decided it, the plan requires the
  opposite (`migration/rust-migration-plan.md:433`: `wait.timeout_seconds`
  bounds the observer while the agent keeps running), and `git log -S` shows the
  line entered in a mechanical crate-splitting commit, not one that reasoned
  about waiting.
- `keychain.rs` claimed the probe was "limited to three seconds like the Python
  runtime". True of the exit wait; not true of the output drain that followed.

**Treat a comment that asserts a bound as a claim to verify, exactly like the
code.** "Deliberately" is a reason to look closer, not a reason to move on.

## The ledger

| # | Site | Class | How it was found | Guarded now by |
|---|---|---|---|---|
| 1 | `xtask/src/deploy.rs` — schema bypass during recovery | **data loss** | writing the T84 cutover crash drill | `t84_recovery_refuses_old_release_after_schema_advance` |
| 2 | `xtask/src/deploy.rs` — stale `.current-next` residue after recovery | recovery correctness | the same drill | `t84_current_pointer_evidence_survives_each_cutover_failure` |
| 3 | `crates/agent-run-platform/src/process.rs` — unverifiable descendant set reported as clean | **false evidence** | a test written for T33 that its author never ran | `an_unverified_descendant_snapshot_is_unknown_not_clean` |
| 4 | `crates/agent-run-adapters/src/lib.rs:64` — Qwen accepted request grants that did not match the role | **permission gap** | closing a scenario wrongly parked as needing a live engine | `qwen_validation_rejects_ungranted_read_roots` |
| 5 | `crates/agent-run-adapters/src/io.rs:287` — reader teardown unbounded | hang | a verification run stalled once and completed once | `process_reap_does_not_wait_for_inherited_pipe_descriptors` |
| 6 | `crates/agent-run/src/transport/socket.rs` — client `wait` unbounded | **hang, user-visible** | a search for the same shape as #5 | `wait_client_deadline_closes_when_broker_holds_response`, `timed_out_wait_retries_the_same_agent_until_terminal` |
| 7 | `crates/agent-run-platform/src/keychain.rs`, `crates/agent-run-core/src/doctor.rs` — output drains unbounded | hang | the same search | `keychain_probe_bounds_stdout_drain`, `ps_probe_bounds_stdout_drain` |

Defect 6 is the one an operator would have met: `agent-run start --wait` and the
MCP `wait` tool hung forever if the broker died mid-wait.

## How they were found, and what that says about the tests

Defect 3 deserves a note. It came from a batch of eight tests whose author
committed them **without executing any of them**; four did not compile. After
the build was repaired, one failed — and it was right to fail. The unverified
work was simultaneously defective and the only source of a genuine finding. The
distinction could only be made by running it.

Defect 5 nearly escaped. A verification run hung once and passed the second
time, which invites "flaky, move on". The tell was one line in the report: 41 of
44 tests had printed `ok` and the 42nd had not. A test runner prints `ok` after
the test returns, so the 42nd never returned — a stall inside the test, not a
flake. It did not reproduce on an idle machine in 18 attempts; the mechanism was
found by reading the code the report pointed at.

## A measurement trap recorded on purpose

While checking whether the last unported behavior was a real divergence, a test
modelled on Python's fixture **passed and was wrong**. The descendant slept 6
seconds — Python's own figure — while this port's bounded waits plus the poll
window covered about the same span, so the descendant reached its natural exit
inside the observation window. Nothing had been killed.

With a 60-second descendant the test fails, which is the true result. Python is
immune because its deadline is 0.5 s and its poll window 0.5 s.

Copying a literal duration without copying the ratio produces a test that agrees
with you. The full account is in `migration/adr/A10-spawn-backend.md`, section
"Measured cost of decision 5".
