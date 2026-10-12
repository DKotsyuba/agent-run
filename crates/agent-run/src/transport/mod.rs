pub mod mcp;
/// Cache hints shared by both MCP `tools/list` handlers.
pub mod mcp_cache;
pub mod mcp_text;
/// Kernel-verified identity of a Unix-socket peer.
pub mod peer;
pub mod socket;
/// Run-bound worker MCP with no operator capabilities.
pub mod worker_mcp;
/// The broker's private writer socket.
pub mod writer_socket;

pub use agent_run_platform::frame;
