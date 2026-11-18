# Process identity

agent-run never treats a PID alone as ownership. Every Rust-started supervisor
records the PID, process group, kernel birth time, and a platform token:

- Linux: boot ID plus `/proc/<pid>/stat` start ticks;
- macOS: kernel process start seconds and microseconds.

The native platform layer classifies an observation as `alive`, `dead`,
`reused`, `unknown`, `denied`, or `not_started`. Exact token or birth-time
equality is required for `alive`. A missing process or zombie is `dead`; a
different birth identity is `reused`. Missing, unreadable, or denied evidence
never becomes death because time elapsed.

During admission, the broker stores its own startup-owner identity. The detached
supervisor reports its exact PID over the bootstrap pipe, commits its native
identity, and only then signals READY. Reconciliation marks an active run lost
only when the stored startup owner or supervisor is observed as dead or reused.

## Signalling and cleanup

The shared harness transport observes the primary child PID independently of
stdout EOF, during startup RPC as well as streaming. On primary exit it sends
TERM immediately to captured live descendants. Queued output drains without
counting downstream processing time as silence; each idle read after exit is
limited to 200ms. Cancellation of a read does not reset its idle deadline.
Captured writers still alive after two seconds receive KILL even if they keep
producing output. There is no overall execution deadline; ownership checkpoints and the bounded
post-exit cleanup path remain responsible for process evidence.
The supervisor then completes bounded escalation and
records the final cleanup proof. Normal protocol completion still follows the
same supervisor cleanup path even if the primary PID has not exited yet.

Group signals require the recorded group leader to be `alive` with its original
identity and group. Captured descendants are also checked individually by PID,
kernel token and birth time, including after the leader exits. Reused, unknown,
denied and missing identities never authorize a signal.

Before group termination, agent-run captures every readable member by PID and
birth identity. Cleanup evidence records the signals attempted, scope,
`group_gone`, nullable `descendants_gone`, and `confirmed`. Confirmation
requires both the original group and every readable captured descendant to be
gone. Captured descendants which left the original group receive individual
signals. This is evidence about observed identities, not proof of a complete
historical process tree: a process may detach before any snapshot sees it.

Cleanup sends TERM to the verified group and captured live descendants immediately.
After a short caller-selected grace it sends KILL to remaining verified identities.
Bounded observation retries then allow exit and transient inspection states to
settle. Current provider supervisors checkpoint newly captured identities in
SQLite during startup RPC, streaming and final cleanup. Recovery restores these
snapshots even when the original leader has exited; every signal still requires
a fresh identity check. Old attempts without snapshots retain the conservative
leader-only recovery path. No migration reconstructs an escaped child that was
never captured, and observations not yet checkpointed when a supervisor crashes
cannot be recovered from an already-dead leader.
If the required proof cannot be established, a provider attempt keeps
its ownership and records a bounded cleanup diagnostic for investigation.

### Linux child subreaper

On Linux each supervisor makes itself the child subreaper
(`PR_SET_CHILD_SUBREAPER`) before it spawns anything. Engine descendants which
double-fork, `setsid`, `nohup` or join another process group then reparent to
the supervisor instead of init, so every termination point (primary exit,
group or descendant signals, cleanup settling) reaches them by parentage.

The supervisor tells its own spawns from adopted orphans only by an explicit
registry. Every child is forked through a spawn gate: before `exec` the child
reports its PID and waits until the supervisor has registered its exact PID and
start token under an admission lock. A child that cannot be registered never
runs its program; the spawn fails and is reaped. The OS launch is a single
standard-library spawn of a command consumed by the gate; wrapping the child
for the async runtime is a separate step. If wrapping fails after the program
started, the engine and its process tree stay owned: the supervisor records
the exact root as the attempt's process (so the attempt is never certified as
never spawned), runs the ordinary bounded cleanup and records a confirmed
cleanup proof only when the tree is proven gone and the engine's status was
collected. Otherwise ownership is retained and the run reports
`cleanup unconfirmed` for recovery. A registration stays until the spawner
collected the exit status. Process groups and PID listings never decide
ownership. An orphan cannot name its tree, so it is attributed to an engine only
when that engine was spawned at a clean boundary: under the same lock and
before the fork, `waitid(P_ALL, WNOHANG | WNOWAIT | __WALL)` reported that the
supervisor had no child at all (of any exit signal, live or zombie), so every later orphan descends from that engine until
another spawn is admitted. Any other engine attributes nothing and keeps the
200 ms polling refresh. Only captured orphans are reaped, with
`waitpid(pid, WNOHANG)` after a fresh check that the exact PID and token are an
unregistered zombie child; `waitpid(-1)` is never used. Every owner does one
last such reap when it is released (end of cleanup, engine transport drop), so
a member that exited after the last observation cannot linger and deny the
next spawn its clean boundary. Orphans that were never captured are not reaped
and remain zombies of the supervisor until it exits.

The engine itself is not made a subreaper: Bun-based (Claude, OpenCode) and
tokio-based (Codex) engines reap only children they spawned, so adopted
orphans would stay zombies inside the engine for its whole lifetime.

With an exclusive engine the mid-run refresh slows to 2 s
(`AGENT_RUN_DESCENDANT_REFRESH_MS`, 200 to 600000): it keeps the durable
crash-recovery capture and orphan reaping current. Without a subreaper (macOS,
or `prctl` refused) the 200 ms polling remains. A Pool peer whose ancestry
includes a recorded supervisor identity (PID and exact start token) is refused
as a Run descendant.

Captured descendants are compared by their native start token; `birth` decides
only for legacy evidence without one. Linux birth is derived from the wall-clock
boot time, so a clock step changes it for a process that computes it later.

macOS uses native process APIs and a start-time-only sysctl fallback when
same-user relationship fields are unavailable. Linux reads `/proc`; its boot ID
prevents start-tick reuse across reboots. Both platforms fail closed when group
or relationship evidence cannot be established.

Supervisors start with session-creating `posix_spawn`, so no parent code runs in
the child between fork and exec. The fork fallback exists only when the C
library explicitly rejects `POSIX_SPAWN_SETSID`; any other spawn error
propagates and an ambiguous creation is never retried through a second backend.
The chosen backend is recorded as launch evidence. On macOS the recorded birth
time is the kernel start timeval as `seconds + microseconds / 1e6`, with no
rounding beyond that division.

Historical rows that contain only a birth-time float remain readable. They can
prove a missing PID dead or a birth mismatch reused, but a present PID without
matching evidence remains unknown.


## Incremental ownership and terminal diagnostics

Harness ownership checkpoints send at most 512 newly captured identities plus the root.
The capture offset advances only after acknowledgement; cancelled or refused writes
keep the same unacknowledged identities. Individual writer requests remain bounded,
but accumulated process history has no two-MiB ceiling. Recovery sends historical
evidence through the same bounded batch shape and store inserts remain idempotent.

Each captured identity may include an executable basename (maximum 128 UTF-8 bytes),
looked up while the same kernel identity is alive. No arguments, environment or full
executable path is stored. Old or unreadable names remain explicitly unknown.
Counts describe observed process starts, not every exec transition or children that
exited before observation.

After terminal status and confirmed cleanup of every owned attempt, one transaction
stores a process_summary_v1 event and deletes detailed attempt process membership,
ownership and discovery snapshots. The summary deduplicates main and discovery process identities and includes the
observed total, the
16 most frequent program basenames/counts, and counts for unknown and other names.
Unconfirmed cleanup retains all ownership evidence. Reconciliation converges old
confirmed terminal records too. Only after a committed summary may filesystem
cleanup remove the corresponding recovery.<attempt>.json process snapshots;
failed executions with a sealed candidate retain its receipt with the ownership
list removed, so the candidate proof is not lost.
Candidate answers, transcripts, failure diagnostics and unresolved command receipts
keep their existing policy. A restart retries interrupted cleanup. No SQL schema
migration is needed: names are optional identity JSON and summaries use events.
