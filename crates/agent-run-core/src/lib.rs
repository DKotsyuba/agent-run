//! Orchestration layer coordinating configuration, persistence, and engines.
pub mod capacity;
pub mod codex;
pub mod delivery;
pub mod dispatch;
/// Session binding and per-turn context hooks shared by CLI hosts.
pub mod hooks;
pub mod lifecycle;
pub mod service;
pub mod stream;
pub mod supervisor;

pub use agent_run_adapters as adapters;
pub use agent_run_config::{config, policy, profiles};
pub use agent_run_domain::{domain, error, Error, Result};
pub use agent_run_platform::{frame, fs, launch, process};
pub use agent_run_store as state;

/// Re-exports answer proof operations at the orchestration boundary.
pub mod verify {
    pub use agent_run_platform::verify::*;
}

/// Stores transcript text in bounded UTF-8 chunks without modifying content.
pub fn journal(
    store: &state::Store,
    id: &domain::AgentId,
    role: &str,
    text: &str,
    name: Option<&str>,
    raw_ref: Option<&str>,
) -> Result<()> {
    let mut remaining = text;
    while !remaining.is_empty() {
        let mut end = remaining.len().min(16 * 1024);
        while !remaining.is_char_boundary(end) {
            end -= 1;
        }
        store.message(id, role, &remaining[..end], name, raw_ref)?;
        remaining = &remaining[end..];
    }
    Ok(())
}
