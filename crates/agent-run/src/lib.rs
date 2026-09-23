//! Binary composition facade for the layered Rust port.
#[cfg(not(unix))]
compile_error!("agent-run currently requires macOS or Linux (Unix sockets and process groups)");

pub use agent_run_adapters as adapters;
pub use agent_run_config::{config, policy, profiles};
pub use agent_run_core::{capacity, delivery, dispatch, doc, doctor, hooks, service, supervisor};
pub use agent_run_domain::{domain, error, Error, Result};
pub use agent_run_platform::{frame, fs, process, verify};
pub use agent_run_store as state;

pub mod cli;
pub mod init;
pub mod launchd;
pub mod migrate;
pub mod transcript;
pub mod transport;
