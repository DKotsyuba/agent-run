//! Orchestration layer coordinating configuration, persistence, and engines.
/// Unit tests reuse the same canonical-import writer harness as integration tests.
#[cfg(test)]
extern crate self as agent_run_core;
pub mod agent_identity;
/// In-process producer lanes of the resident broker.
pub mod broker_writer;
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
/// Attempt-owned service and model ping recovery.
pub mod health;
/// Session binding and per-turn context hooks shared by CLI hosts.
pub mod hooks;
pub mod housekeeping;
pub mod lifecycle;
pub mod logging;
pub mod managed_services;
/// Single-instance storage of Codex's unindexed native metadata caches.
pub mod native_cache;
/// Protected native owner admission, without a separate input or delivery subsystem.
pub mod native_owner;
/// Freeze/thaw lifecycle for native Codex directory caches in retained homes.
pub mod native_tree_cache;
/// Owned OpenCode v2 native runner.
pub mod opencode;
/// Persistence facade of processes other than the resident broker.
pub mod persist;
/// Authenticated workdir-confined report writes for restricted research.
mod research_reports;
/// Native cache lifecycle glue shared by the supervisor and operator surfaces.
pub mod runtime_cache;
/// Filesystem relocation of sealed managed trees into the shared store.
pub mod runtime_storage;
pub mod service;
/// Client lanes of the private writer socket.
pub mod storage_client;
/// Reference-aware collection of the shared managed-asset store.
pub mod storage_gc;
/// Binary framing of the private writer socket.
pub mod storage_wire;
pub mod stream;
pub mod supervisor;
pub mod worker_finish;
/// The single resident writer service and its submission handle.
pub mod writer;
/// Connection and frame-assembly admission of the private writer socket.
pub mod writer_limits;
/// Server side of one writer-socket connection.
pub mod writer_server;
/// One shared fixture writer transport for all in-crate unit tests.
#[cfg(test)]
#[path = "../tests/common/writer.rs"]
mod writer_test_harness;

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
