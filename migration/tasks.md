# Rust migration execution board

This board is the executable decomposition of the authoritative plan at Python baseline `0d28b79`. Task targets use the post-M05 workspace from plan §4.1; dependency direction follows plan §4.2, gates follow plan §18, and task completion follows plan §19.1/§19.4. Detailed scope, tests, status, and acceptance commands are in `migration/tasks.csv`.

Current shared HEAD includes all six parity inventories, including `migration/inventory/engines.md` at `522697240498a9829faf6b88e36738834c993500`; no row is `inventory-pending`. M01-M08 and M18 are already `in_progress`. M18 may port migration bodies now, but its final integration acceptance waits for M17's store/schema API.

## Lanes and ownership

The division applies plan §19.3 and the dependency rules in plan §4.2. A lane owns the listed paths; another lane consumes its public API or schedules an explicit handoff instead of editing those files concurrently.

| Lane | Owned crates/files | Shared-file rule or hazard |
|---|---|---|
| baseline/control | `migration/baseline/`, `migration/compatibility.csv`, `tests/fixtures/baseline/`, differential harness | Fixture indexes have one owner through M04; consumers add tests, not fixture metadata. |
| platform/artifacts | `agent-run-platform`, safe paths/files, process identity, canonical bytes | M23 owns safe-file/path APIs used by store and adapters; do not duplicate filesystem checks. |
| config/roles | `agent-run-domain`, `agent-run-config`, role/profile/native-setting contracts | M11 exclusively owns shared DTOs and the error enum; M12 exclusively owns the tool registry and schema asset. |
| store | `agent-run-store`, `sql/schema.sql`, `sql/migrations/` | M17 owns schema assets and M18 owns migration bodies. Capacity, delivery, and hooks use store APIs from M20d/M21/M22b. |
| core lifecycle | `agent-run-core` service, preparation, launch, supervisor, lifecycle, resume | `service.rs` has one owner through M30d. Adapters return evidence; they never choose terminal status or access SQL. |
| codex adapter | `agent-run-adapters/src/codex/` and Codex fixtures | Generated-home files use M23/M25 APIs. Shared adapter traits change only by a short core/config contract handoff. |
| claude/glm adapters | `agent-run-adapters/src/{claude,glm}`, shared adapter I/O/redaction | M14b/M14c own environment/auth boundaries; M35d owns shared validation; capacity's OmniRoute reader is not owned here. |
| transports/CLI | `crates/agent-run/src/{cli,transport}`, socket/MCP tests | M12's registry is the sole tool list. Transport errors map M11 codes and never create a second execution host. |
| capacity | `agent-run-core/src/capacity/` and capacity tests | M43b is the sole OmniRoute quota reader; it must not read launch credentials. Store persistence is M20d. |
| delivery/hooks | `agent-run-core/src/delivery/`, `crates/agent-run/src/hooks/` | Store lease/evidence transitions are M21; late binding is M22b. M09 decides the native relay boundary before M48. |
| operations/release | doctor/logging/init/docs/launchd, `xtask/`, workflows, release evidence | Release tooling consumes embedded assets and stable APIs. It must not patch schemas or runtime contracts during packaging. |

Exclusive shared ownership is therefore: DTOs and public error enum → M11; schema and migration assets → M17/M18; tool registry and `assets/tools.json` → M12; service composition → M30d; safe filesystem/path APIs → M23; baseline fixture index → M04.

## Waves

The current in-flight set is M01-M08 and M18. It is not reassigned. Each planned wave has at most six tasks; a wave starts after its listed predecessors and required in-flight artifacts land. Within a wave, tasks are intended to run concurrently in separate temporary homes and databases as required by plan §19.3.

Wave 1 is immediately startable against the M05 workspace worktree. Its only unfinished prerequisites are explicitly in-flight items above.

| id | title | lane | prerequisites | size |
|---|---|---|---|---|
| M09 | Spike native Rust Desktop host capability | delivery/hooks | M05 | M |
| M10 | Spike MCP negotiation errors cancellation and EOF | transports/CLI | M05 | M |
| M11 | Define domain newtypes FSM errors and view DTOs | config/roles | M05 | M |
| M14b | Match host-environment deny-list inheritance | claude/glm adapters | M05 | S |
| M23a | Provide anchored no-follow file and agent-path primitives | platform/artifacts | M05 | M |
| M27 | Build a fault-controlled Rust fake engine | core lifecycle | M05 | M |

Wave 2 establishes the contract, DB, filesystem, and process seams needed by the first working slice in plan Appendix D.

| id | title | lane | prerequisites | size |
|---|---|---|---|---|
| M12 | Create the single 11-tool registry and golden schemas | config/roles | M03; M11 | M |
| M13a | Port strict TOML and built-in adapter aliases | config/roles | M03; M11 | L |
| M14c | Add GLM Keychain credential fallback | claude/glm adapters | M05 | M |
| M17 | Create schema v16 and thread-owned store connections | store | M04; M11 | L |
| M23b | Provide durable publication and fault-injection seams | platform/artifacts | M23a | M |
| M26 | Observe process identity and verify group cleanup evidence | platform/artifacts | M06; M07 | L |

Later waves, in dependency order:

| Wave | Concurrent tasks |
|---|---|
| 3 | M13b, M14a, M15a, M15b, M20b, M24 |
| 4 | M16, M19, M20a, M20c, M20d, M35d |
| 5 | M21, M22, M25, M28a, M35b, M43d |
| 6 | M28b, M28c, M30a, M30b, M43a, M43b |
| 7 | M22b, M29, M30c, M44, M46, M47 |
| 8 | M30d, M31a, M32a, M35a, M39, M48 |
| 9 | M32b, M32c, M34a, M36, M37, M40 |
| 10 | M31b, M33, M38, M41a, M42, M43c |
| 11 | M34b, M35c, M41b, M45, M49 |
| 12 | M50a, M50b, M54 |
| 13 | M51, M52 |
| 14 | M53 |
| 15 | M55 |
| 16 | M56 |

The priority chain is Appendix D's `M17/M19 → M23/M24 → M26-M30 → M39/M41a` slice: CLI start → broker → durable starting row → separate supervisor → READY → fake engine → journal → proof v2 → terminal answer. In parallel, M13a/M14a cover existing config/account homes, M18 opens historical DBs, M44 restores daily capacity order output, and M12/M39/M42 preserve CLI/socket/MCP callers.

## Conflicts and resolutions

- OmniRoute appears in both capacity inventory WP CD-2 and engines WP EN-2. Treat them as one task, M43b, owned by capacity; adapter auth from EN-1 is launch-only and never crosses the quota-reader boundary. See `migration/inventory/capacity-delivery.md` §5 and `migration/inventory/engines.md` §5.
- Codex WP CX-2 and state WP ST-10 both suggest filesystem/path work near their current modules. Plan §4.2 makes platform the owner, so M23 provides one API and adapter/store tasks only call it. See `migration/inventory/codex-adapter.md` §5 and `migration/inventory/state.md` §5.
- State WP ST-4 could not locate the `waiting_binding` origin, while delivery WP CD-7 defines that lifecycle. M21 owns atomic store creation, M22b owns binding/receipt transitions, and M46 owns dispatch policy. See both inventories §5.
- State reports error-wire classification as unverified and transports requires exact public errors. M11 owns machine codes/public messages; M39/M42 only map them. `AnswerIntegrityError` must not be silently collapsed to validation. See `migration/inventory/state.md` §6 and `migration/inventory/transports.md` §6.
- Current Rust detached launch uses pre-exec behavior, while plan §8 and core WP CO-3 require a `posix_spawn`-first ownership protocol. M06/M07 provide evidence, M26 owns identity/cleanup, and M28b implements the selected A10 result.
- Engines §6 found no evidence for the suggested read-only Claude/GLM Bash gap; Python tests assert Bash exclusion. Preserve that baseline unless the owner supplies a concrete external profile and failing case.
- Delivery currently ships a CJS Desktop bridge, but plan §1.3/§14.5 requires a native replacement for a full Rust release. M09 is a gate, not permission to retain CJS as installed runtime; M48 follows its evidence.
- The current Rust schema says v16 but does not prove historical migration compatibility. M17 is the sole schema owner and M18 remains incomplete until T14/T16/T17 pass against the M04 corpus. See state inventory §6 and plan §7.

## Owner decisions

These decisions follow plan §23; defaults keep implementation moving but do not waive their evidence gates.

| Decision | Recommended default |
|---|---|
| A11 `required_constraints` | Use explicit `role ∪ request` tightening, persist it in the frozen role plan, and document it as an intentional compatibility change. |
| Read-only Claude/GLM Bash | Preserve Python's tested Bash exclusion; reopen only with the exact external profile and a reproducible mismatch. |
| A09 Desktop relay path | Require a direct native Rust host path proven by M09. If host admission cannot be obtained, mark native Desktop cutover blocked; keep CJS only as a development oracle fixture. |
| A10 spawn backend | Choose `posix_spawn` with session creation on supported platforms; permit a documented fallback only when the capability is unavailable and the same identity/reap tests pass. |
| A12 historical serialization | Emit exact Python bytes for hash-bound historical formats; introduce any new encoding only as a versioned dual-read format. |
| A13 external Python adapters | Preserve built-in aliases; do not emulate arbitrary Python imports. Add a preflight inventory and block affected installations until a native adapter or explicit scope decision exists. |
| A14 MCP SDK/protocols | Select rmcp features by the recorded client interop matrix from M10, not by matching SDK version numbers. |
| A15 supported targets | Initially claim only macOS and Linux architecture/version combinations with CI plus native evidence; unsupported combinations fail explicitly. |
| A16 large weights/nonfinite values | Reject overflow, NaN, infinity, and unsafe magnitudes with the M11 typed validation error before ranking. |

No production home, native session, credentials, or deployment target is authorized by this board. P12 cutover still requires the separate authorization in plan §18.12/§21.

## `implemented` is not `verified`

Plan §19.1 lists five statuses — `planned`, `in_progress`, `implemented`,
`verified`, `blocked` — and states the rule plainly: **"`implemented` не равно
`verified`. При отсутствии live-проверки используется явный отдельный флаг, а не
скрытый зелёный статус."**

This is that explicit flag.

At this revision `migration/tasks.csv` holds 82 rows:
80 `implemented`, 2 `planned`
(M09, M55), and **0 `verified`**.

**A count of implemented rows is therefore not a readiness figure.** "80 of
82" says that the code exists and its own acceptance command passes. It
does not say the behavior was confirmed against a real engine, a real
notification host, or a second operating system.

No row can honestly become `verified` yet, and the reason is not oversight:

- Four qualification scenarios retain three live resource classes that a fixture
  cannot supply — an installed supported engine, the running ChatGPT Desktop host,
  and one non-macOS machine. They are named in `migration/evidence/qualification-scope.md`.
- M55 is the row that would establish that evidence, and it is `planned` because
  live qualification needs the owner's authorization, not more implementation.
- Risk K01 in plan §22 is blocking and states the consequence directly: without
  that evidence, full parity is not claimed.

Check the tally yourself rather than trusting this paragraph:

```sh
python3 -c "
import csv, collections
rows = list(csv.DictReader(open('migration/tasks.csv')))
print(collections.Counter(r['status'] for r in rows))
print([r['id'] for r in rows if r['status'] != 'implemented'])
"
```
