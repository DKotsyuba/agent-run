pub mod mcp;
/// Cache hints shared by both MCP `tools/list` handlers.
pub mod mcp_cache;
pub mod mcp_text;
pub mod socket;
/// Run-bound worker MCP with no operator capabilities.
pub mod worker_mcp;

pub use agent_run_platform::frame;
