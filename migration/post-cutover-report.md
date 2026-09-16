# Post-cutover report — the record that makes a rollback possible

Status: **blank form.** The plan (§18.12) names a post-cutover report as an exit
artifact for P12. This is that form. Nothing in it is filled in, and it must not
be filled in before a cutover actually happens.

Fill it **while** you cut over, not afterwards from memory. Everything here is
either something you can only observe at the time, or something you will need at
2am when deciding whether to roll back — the version that was current before, the
jobs that were actually loaded, where the backup went.

`migration/recovery.md` describes what the commands do. This records what they
did, once, to this installation.

---

## A. Preflight record (§21.2)

Captured **before** any change. Plan §21.2 requires each of these to be saved,
and today they have nowhere else to live.

| Item | Value |
|---|---|
| Date and operator | |
| Candidate commit (SHA) | |
| Candidate release path | |
| Current release path, before | |
| Current binary version, before | |
| Candidate binary version | |
| Store schema version, before | |
| launchd jobs **actually loaded** before (not "configured") | |
| Config paths in use | |
| Rollback package location | |
| Free space on the backup volume | |
| Concurrent release operation confirmed absent | |
| Manifest readable and checksums verified | |

Native runtime histories and external auth references live outside the database.
Record which ones this installation depends on — restoring a database without
them does not restore the ability to resume:

| External dependency | Path or identifier | Needed for |
|---|---|---|
| | | |

Credentials are deliberately **not** copied into the backup. Naming which
credential stores are required is not the same as copying them; name them.

## B. Quiescence (§21.3)

| Check | Result |
|---|---|
| New admission closed by the normal maintenance mechanism | |
| Existing runs finished, or explicitly cancelled by a recorded decision | |
| Periodic capacity/delivery writers stopped | |
| Legacy workflow writers checked **by process birth**, not by status alone | |
| Any unknown or access-denied writer treated as live, not assumed dead | |
| Broker stopped in the correct order | |
| SQLite Backup API snapshot taken to a private retained backup | |
| Snapshot integrity and schema checked | |

An unknown or denied writer may not be automatically declared dead. If one was
encountered, record what it was and how it was resolved:

```
```

## C. Cutover steps (§21.4)

Steps **C5, C6 and C7 are outside `cargo xtask` by decision** (ADR A19: service
restore, API readiness wait, and release-time smoke are declared divergences).
For those three, record what you did by hand or with which other tool.

| Step | Action | Evidence observed | Done by |
|---|---|---|---|
| C1 | private deployment journal written with old/new release, backup, phase | journal readable after an abrupt stop | `xtask` |
| C2 | DB state checked; only genuinely needed migrations run | version and integrity match target; no-op for v16→v16 | `xtask` |
| C3 | candidate release fully sealed | binary, resources and manifest agree | `xtask` |
| C4 | `current` pointer switched atomically | pointer names a whole immutable release | `xtask` |
| C5 | previously loaded services/jobs restored | jobs disabled before the update are **not** enabled now | **operator** |
| C6 | API readiness, schema, tools, doctor checked | liveness and capability discovery match target | **operator** |
| C7 | isolated smoke run, answer proof checked | no user conversation touched | **operator** |
| C8 | MCP hosts reconnected normally | new proxy uses the target binary; old hosts not killed arbitrarily | **operator** |
| C9 | deployment marked committed; backup and old release retained | a reproducible recovery path exists | `xtask` |

## D. Outcome

| Item | Value |
|---|---|
| Current release path, after | |
| Current binary version, after | |
| Store schema version, after | |
| Journal final phase | |
| Backup retained at | |
| Old release retained at | |
| Time from quiescence to C9 | |

## E. Anything that did not go to plan

Record every deviation, including ones that turned out harmless. A deviation
that is not written down is one the next operator will rediscover.

```
```

## F. Rollback readiness, stated explicitly

Answer these **now**, while the facts are fresh — not when you need them.

| Question | Answer |
|---|---|
| Did the schema move forward during this cutover? | |
| If it did: rollback is **not** available; roll-forward or a corrected compatible build is the path (§21.5) | |
| If it did not: which exact release would a rollback restore? | |
| Which runs completed **after** the backup was taken, and would an explicit restore discard them? | |
| Who decided that, and when? | |

A retained schema version does not prove semantic compatibility of every payload
version. If a rollback is claimed possible, say what was checked beyond the
version number.

## G. Signature

```
Cutover performed by: _______________________________
Date and time: ______________________________________
Production home touched: [ ] yes  [ ] no (copy only)
Separate authorization for production recorded: ______
```

A cutover to a copy of the environment is a rehearsal and must be marked as one.
The plan requires the rehearsal first, and a separate authorization before a
production home is touched at all.
