//! Cache hints for `tools/list`, shared by the operator and worker MCP surfaces.
//!
//! MCP 2026-07-28 clients require `ttlMs` and `cacheScope` on list results;
//! earlier sessions must stay byte-identical, so the hints are added only when
//! the request negotiated 2026-07-28 or later.

use rmcp::{
    model::{CacheScope, ListToolsResult, ProtocolVersion, Tool},
    service::{RequestContext, RoleServer},
};

/// How long, in milliseconds, a client may treat a tool list as fresh.
pub const TOOLS_LIST_TTL_MS: u64 = 60_000;

/// True when the request negotiated MCP 2026-07-28 or later (ISO dates compare as strings).
fn is_modern(context: &RequestContext<RoleServer>) -> bool {
    context
        .protocol_version()
        .is_some_and(|v| v.as_str() >= ProtocolVersion::V_2026_07_28.as_str())
}

/// Build the complete, unpaginated tool list, adding private cache hints only for modern requests.
pub fn tools_list_result(
    context: &RequestContext<RoleServer>,
    tools: Vec<Tool>,
) -> ListToolsResult {
    let result = ListToolsResult::with_all_items(tools);
    if is_modern(context) {
        result
            .with_ttl_ms(TOOLS_LIST_TTL_MS)
            .with_cache_scope(CacheScope::Private)
    } else {
        result
    }
}
