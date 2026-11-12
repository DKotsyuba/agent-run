//! Library surface for xtask's release/deploy logic.
//!
//! `main.rs` is a thin CLI shim over these modules; splitting them into a
//! library target lets the `xtask/tests/*.rs` integration modules (one `it`
//! binary) exercise the same code the CLI calls, for example
//! `cargo nextest run -p xtask -E 'test(/^(release|deploy_recovery)::/)'`.
pub mod archive;
pub mod deploy;
pub mod family;
/// Test steps of the repository gate (nextest, doctests, tmpfs scratch).
pub mod gate;
pub mod installer;
pub mod release;

/// External release inventory and observed acceptance evidence.
pub mod delivery;

/// Bounded tar validation before extraction or execution.
pub mod tar_guard;

/// Network preparation and local release/payload checks.
pub mod release_ops;

/// Read-only exact release observer and durable terminal events.
pub mod release_wait;

/// Explicit staged publication of the already accepted immutable inventory.
pub mod release_publish;
