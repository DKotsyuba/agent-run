# A10 — Spawn backend and process birth precision

Status: accepted for the Rust port (spike M06 + M07, plan §8.2–§8.7, tests T25/T28/T30/T31/T36).
Platform of record for this spike: macOS 27.0.0 (Darwin, arm64), rustc 1.99.0-nightly, libc 0.2.189.

## Decision

1. **Spawn backend: `posix_spawn` with `POSIX_SPAWN_SETSID`, via `libc`.**
   `rust/src/launch.rs` builds argv, an environment snapshot, file actions and
   spawn attributes in the parent and lets the kernel create the session. No
   Rust code runs in the child between fork and exec, which is what the
   multithreaded daemon requires (AGENTS.md invariant, plan §8.3).
   - Attributes: `POSIX_SPAWN_SETSID` (value `0x0400` on macOS, taken from
     `<sys/spawn.h>` because libc 0.2.189 exports it only for Linux),
     `POSIX_SPAWN_SETSIGDEF` (SIGPIPE) and `POSIX_SPAWN_SETSIGMASK` (empty),
     plus `POSIX_SPAWN_CLOEXEC_DEFAULT` on macOS so only the named descriptors
     reach the child.
   - Descriptors: the three bootstrap pipe ends are duplicated to numbers ≥ 10
     with `F_DUPFD_CLOEXEC`, then `adddup2`'d onto child fds 3 (ready), 4
     (identity) and 5 (error), which are also named on the command line
     (`--ready-fd/--identity-fd/--error-fd`). Sources above 9 can never collide
     with a target, and `dup2` clears close-on-exec on the target on every
     supported libc. Child stdio is `/dev/null` (`addopen`).
2. **Fallback only when the flag is explicitly rejected.** If
   `posix_spawnattr_setflags` returns `EINVAL` (old glibc without
   `POSIX_SPAWN_SETSID`), `fork_setsid` runs `setsid`, `dup2`, `open`,
   `signal`, `sigprocmask` and `execve` — async-signal-safe calls only, on
   memory prepared before the fork — and writes a fixed-size exec-failure
   record to fd 5 before `_exit(1)`. Any other spawn error propagates: an
   ambiguous creation is never retried through a second backend.
3. **Chosen path is recorded as launch evidence.** `SpawnBackend` serializes as
   `posix_spawn_setsid` / `fork_setsid` and is written into
   `agents.startup_owner_pid_identity` next to the provisional PID identity,
   before any proof (`rust/src/supervisor.rs`).
4. **Process birth identity: kernel start time, Python-compatible.**
   macOS reads `proc_pidinfo(PROC_PIDTBSDINFO)` and falls back to
   `sysctl(KERN_PROC_PID)` — psutil's own source — when that is denied;
   Linux reads `/proc/<pid>/stat` starttime plus `/proc/stat` btime. Stored
   float `birth` equals psutil's `create_time()` bit for bit; the extra
   `darwin:<sec>:<usec>` / `linux:<boot_id>:<ticks>` token is Rust-only.
5. **Verdicts are an enum** (`ProcessState`): `Alive`, `Dead`, `Reused`,
   `Unknown`, `Denied`, `NotStarted`. Only `Dead`/`Reused` may mark a run lost;
   only `Alive` with a leader-owned group authorizes a signal. Token evidence
   compares exactly; legacy Python rows (float only) compare with `==`, exactly
   as `observe_process` does.

## Alternatives rejected

| Alternative | Why not |
|---|---|
| `std`/`tokio` `Command` with a `pre_exec` `setsid` closure (the code this replaces) | Runs Rust in the forked child of a multithreaded daemon; `std` also cannot use posix_spawn once `pre_exec` is set. |
| `Command::process_group(0)` | Creates a group, not a session; the supervisor would keep the daemon's controlling terminal and session. |
| Separate single-threaded helper binary that calls `setsid` after exec | Extra process, extra failure mode, and an exec'd child cannot become a session leader if it is already a group leader. |
| Keep `psutil`-style birth only (no token) | Loses immunity to wall-clock steps; the token costs nothing and legacy rows are still compared by float. |
| Treat `Denied`/`Unknown` as death after an age threshold | Plan §8.4 forbids it: elapsed age never upgrades an uncertain observation. |
| macOS `posix_spawn_file_actions_addinherit_np` instead of high-fd `adddup2` | Apple-only; the `adddup2` form is portable and already collision-free. |

## Consequences

- Bootstrap is a four-message protocol: identity line (child PID), optional
  JSON failure record, READY (`ready` / `fail:<reason>`), and the reap status.
  One 30 s budget covers exec through READY, as in Python.
- Failures after spawn either reap the child or report
  `detached supervisor process group survived cleanup`; only a verified
  leader/group whose exact identity is still `Alive` is ever signalled.
- Python's payload pipe has no Rust counterpart: the Rust supervisor reads its
  run from the durable store, so no secret is passed at launch.
- The exec-failure record is `{"stage":"exec","type":"OSError","message":"exec failed","errno":N}`;
  Python names the concrete exception class and `strerror` (not async-signal-safe in Rust).
- One blocking reaper thread per live supervisor (Python's default behaviour).
- macOS `sysctl` fallback yields birth and token only; `ppid`/`group` stay `0`
  (unknown) and every ownership check therefore fails closed for processes
  owned by other users.

## Evidence

Commands (worktree `.wt/spike-process`, `CARGO_HOME=rust/.cargo-home`, `AGENT_RUN_TEST_TMP=/tmp/claude`):

```
cargo build --offline --all-targets --all-features        # ok, only pre-existing warnings
cargo test  --offline --all-features --no-fail-fast       # launch 10/10, process_identity 5/5,
                                                          # capacity 14, domain_config 20, protocol 11,
                                                          # state 14, verification 14; end_to_end 0/3 (pre-existing)
cargo clippy --offline --all-features --tests             # no warnings in launch.rs/process.rs/supervisor.rs/tests
```

Differential birth observation against psutil 7.2.2 on CPython 3.14.3
(`/Users/pluto/projects/agent-run/.venv-py314/bin/python`), probe
`repr(psutil.Process(pid).create_time())` handed to
`AGENT_RUN_DIFF_PID`/`AGENT_RUN_DIFF_BIRTH`:

```
own child   pid=54255 psutil=1789514359.089212 rust=1789514359.089212 bits=0x41daaa749dc5b5a6 (both) token=darwin:1789514359:89212
root-owned  pid=562   psutil=1789426472.891294 rust=1789426472.891294 bits=0x41daaa1eca390af6 (both) token=darwin:1789426472:891294
```

The root-owned PID is what forced decision 4: `proc_pidinfo(PROC_PIDTBSDINFO)`
returns `EPERM` for another user's process, so before the `sysctl` fallback a
PID reused by a root process read as `Denied` instead of `Reused` — safe
against signalling, but it would have hidden real PID reuse from reconciliation.

Session/group and cleanup evidence comes from `rust/tests/launch.rs`
(`getsid(pid) == getpgid(pid) == pid`, wrapper plus grandchild killed on READY
timeout, exact `WEXITSTATUS` from the reaper, no signal for a stale identity).

`end_to_end` failures are pre-existing and unrelated: the broker refuses to
start in this sandbox, reproduced outside cargo with
`agent-run --home <tmp> api serve` → `{"error":{"kind":"IOError","message":"local I/O operation failed"}}`, exit 2.

## Remaining verification (Linux, not run here)

1. `POSIX_SPAWN_SETSID` (glibc ≥ 2.26) and the `EINVAL` fallback trigger on an
   older glibc; musl behaviour of the same flag.
2. `adddup2` onto fds 3–5 and `/dev/null` stdio under glibc and musl, including
   that no unrelated daemon descriptor leaks (Linux has no `CLOEXEC_DEFAULT`).
3. `/proc/<pid>/stat` + `btime` birth equals psutil's
   `starttime / CLOCK_TICKS + boot_time()` bit for bit, and how far `btime`
   drifts under NTP steps (psutil re-reads it on every call).
4. `hidepid=2` mounts: `/proc/<pid>` of another user's process is `ENOENT`,
   which this code reads as `Dead`; psutil reports `NoSuchProcess` the same way,
   so both sides must be checked against a real hidepid host.
5. macOS only: psutil's `adjust_proc_create_time` shifts `create_time` by whole
   seconds after a wall-clock step; rows written by such a Python process will
   not compare equal to the raw kernel value. Rust-written rows carry the token
   and are immune; legacy rows are not.
