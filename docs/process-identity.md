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
TERM immediately to captured live descendants. Already-written output drains
for at most 200ms; a descendant holding an inherited descriptor cannot keep the
run waiting indefinitely. This deadline survives cancellation of a read by
supervisor maintenance. The supervisor then performs bounded escalation and
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
settle. Captured identities currently live in the supervisor's memory; recovery
after its loss cannot reconstruct an escaped child from an already-dead leader.
If the required proof cannot be established, a provider attempt keeps
its ownership and records a bounded cleanup diagnostic for investigation.

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
