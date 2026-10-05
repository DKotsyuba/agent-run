//! Orchestration layer coordinating configuration, persistence, and engines.
pub mod agent_identity;
pub mod capacity;
pub mod codex;
/// Durable command result handling shared by supervisor and engine runners.
pub mod commands;
pub mod continuity;
pub mod delegation_guide;
pub mod delivery;
pub mod dispatch;
pub mod doc;
pub mod doctor;
/// Session binding and per-turn context hooks shared by CLI hosts.
pub mod hooks;
pub mod housekeeping;
pub mod lifecycle;
pub mod logging;
pub mod managed_services;
/// Single-instance storage of Codex's unindexed native metadata caches.
pub mod native_cache;
/// Freeze/thaw lifecycle for native Codex directory caches in retained homes.
pub mod native_tree_cache;
/// Native cache lifecycle glue shared by the supervisor and operator surfaces.
pub mod runtime_cache;
/// Filesystem relocation of sealed managed trees into the shared store.
pub mod runtime_storage;
pub mod service;
/// Reference-aware collection of the shared managed-asset store.
pub mod storage_gc;
pub mod stream;
pub mod supervisor;

pub use agent_run_adapters as adapters;
pub use agent_run_config::{config, policy, profiles};
pub use agent_run_domain::{Error, Result, domain, error};
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

/// Stores redacted native tool text in bounded UTF-8 chunks with shared
/// invocation identity and optional explicit error/provenance. An empty native
/// tool result still records evidence. The caller owns native interpretation;
/// storage validates provenance, ownership and spool guards. Errors propagate.
pub fn journal_with_error(
    store: &state::Store,
    id: &domain::AgentId,
    role: &str,
    text: &str,
    name: Option<&str>,
    raw_ref: Option<&str>,
    error: Option<(bool, &str)>,
) -> Result<()> {
    let mut remaining = text;
    loop {
        let mut end = remaining.len().min(16 * 1024);
        while !remaining.is_char_boundary(end) {
            end -= 1;
        }
        store.message_with_error(id, role, &remaining[..end], name, raw_ref, error)?;
        remaining = &remaining[end..];
        if remaining.is_empty() {
            break;
        }
    }
    Ok(())
}
