# SQLite access implementation plan

Online database writes must share one authoritative connection without
blocking process observation. Reuse existing typed SQL operations and preserve
durability, request identity, ownership and cleanup. Publication and installation
require their own qualification; a compiling library alone proves neither.

## Minimum architecture

Add `sqlite-access`, an application-independent workspace crate. It owns the
tokio-rusqlite connection, bounded admission, cancellation-safe job lifetime,
read-only opening, writer exclusion, transaction boundaries and diagnostics.
It depends on no agent-run domain/config/platform crates. Existing agent-run
SQL, authorization, process identity and explicit-finish rules remain in the
application store adapter. Local closures are never accepted over a socket.

All online mutations go to the existing resident broker, including local
broker work. Other processes use read-only non-migrating connections and
authenticated typed requests. Offline maintenance requires broker exclusion
and proven process quiescence. No direct-write fallback is allowed.

## Delivery sequence

1. Reuse existing typed SQL units and their authorization/receipt checks.
2. Implement and independently test the reusable crate with tokio-rusqlite
   0.8.0. Bound accepted operations and bytes before its unbounded channel;
   retain permits until SQL finishes even if the caller disappears.
3. Adapt application SQL to the library-owned connection. Keep one complete
   business transaction and its durable receipt atomic. Never retry a native
   action or an ambiguous commit. Route every producer through the broker.
4. Close the original audit defects with targeted regressions:
   F1 read-first no-op turn/catalog; F2 SQL off runtime threads plus responsive
   process observation; F3 statistics outside terminal transaction; F4 atomic
   persistence of already-observed native outcomes; F5 policy/filesystem work
   outside delivery claims; F6 WAL-aware diagnostics; F7 bounded durable
   operation/wait/attempt evidence; F8 visible deferred-work failures.
5. Independently review fault/overload/cancellation/restart behavior and verify
   actual receipts against the tested source.
6. Run isolated synthetic mixed-process load at 1/8/32/64 producers, sustained
   workload and bursts; assert counts, no duplicates, bounded buffering,
   neighbor progress, terminal/cleanup/resume invariants and honest failures.
   Then run required project checks with direct exit receipts.

## Library decision

Selected: tokio-rusqlite 0.8.0, MIT, official registry/docs and upstream
programatik29/tokio-rusqlite agree. Its declared dependencies are rusqlite
0.40.1-compatible, Tokio 1 and crossbeam-channel 0.5; no package build hook.
Existing workspace rusqlite/Tokio versions are retained. Crossbeam-channel
must be at least 0.5.15 (RUSTSEC-2025-0024). Downloads/independent adopters
were unavailable; upstream observed 130 stars/38 forks, with documented API
and source. No tokio-rusqlite advisory was found in the RustSec listing;
this is not a guarantee against unknown defects. Registry API HTTP 403 is
retained as unavailable metadata, not silently claimed verified.

Sources: https://docs.rs/tokio-rusqlite/0.8.0/tokio_rusqlite/
https://docs.rs/crate/tokio-rusqlite/0.8.0/source/Cargo.toml
https://github.com/programatik29/tokio-rusqlite
https://rustsec.org/advisories/RUSTSEC-2025-0024.html

The library supplies the worker thread and serialized connection execution.
Thin project glue remains necessary for bounded admission and cross-process
typed requests; application receipts and ownership cannot be delegated to it.
