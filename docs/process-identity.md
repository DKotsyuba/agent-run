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

Signals are allowed only while the recorded supervisor identity is `alive` and
the supervisor is still the leader of its recorded process group. Reused,
unknown, denied, and missing identity never authorize a signal.

Before group termination, agent-run captures every readable member by PID and
birth identity. Cleanup evidence records the signals attempted, scope,
`group_gone`, nullable `descendants_gone`, and `confirmed`. Confirmation
requires both the original group and every readable captured descendant to be
gone. Escaped descendants are not signalled individually, and group
disappearance alone does not claim wider tree cleanup.

macOS uses native process APIs and a start-time-only sysctl fallback when
same-user relationship fields are unavailable. Linux reads `/proc`; its boot ID
prevents start-tick reuse across reboots. Both platforms fail closed when group
or relationship evidence cannot be established.

Historical rows that contain only a birth-time float remain readable. They can
prove a missing PID dead or a birth mismatch reused, but a present PID without
matching evidence remains unknown.
