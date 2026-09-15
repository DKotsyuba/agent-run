//! Rust migration of agent-run. All runtime code lives in this crate.
//! Integration/parity status is recorded honestly in docs/MIGRATION_STATUS.md.
#[cfg(not(unix))]
compile_error!("agent-run currently requires macOS or Linux (Unix sockets and process groups)");
pub mod adapters;
pub mod capacity;
pub mod cli;
pub mod config;
pub mod delivery;
pub mod dispatch;
pub mod domain;
pub mod error;
pub mod fs;
pub mod policy;
pub mod profiles;
pub mod process;
pub mod service;
pub mod state;
pub mod supervisor;
pub mod transport;
pub mod verify;
pub use error::{Error, Result};
