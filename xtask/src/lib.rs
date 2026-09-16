//! Library surface for xtask's release/deploy logic.
//!
//! `main.rs` is a thin CLI shim over these modules; splitting them into a
//! library target lets `xtask/tests/*.rs` integration tests exercise the
//! same code the CLI calls (`cargo test -p xtask --test release` and
//! `--test deploy_recovery`, per `migration/tasks.csv` M52/M53).
pub mod archive;
pub mod deploy;
pub mod evidence;
pub mod release;
