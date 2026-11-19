//! Immutable Pool chat admission data and the private worker catalog method.

use crate::{Result, StoreId, error::invalid};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Pool chat v1's fixed worker tool names. Pool owns the tool schemas.
pub const TOOLS: [&str; 7] = [
    "pool_join",
    "pool_get",
    "pool_post",
    "pool_send_member",
    "pool_notify_owner",
    "pool_propose",
    "pool_vote",
];

/// Authenticated broker method used during worker MCP startup.
pub const CATALOG_METHOD: &str = "worker/pool_chat_catalog";

/// Seat association fixed when a new lineage is admitted.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PoolChat {
    pub version: u32,
    pub pool_id: String,
    pub seat_id: String,
    pub pool_store_id: StoreId,
}

impl PoolChat {
    /// Rejects unsupported contracts before admission or relay.
    pub fn validate(&self) -> Result<()> {
        if self.version != 1 {
            return Err(invalid("unsupported pool_chat version"));
        }
        for id in [&self.pool_id, &self.seat_id] {
            if Uuid::parse_str(id).is_err() {
                return Err(invalid("pool_chat pool_id and seat_id must be UUIDs"));
            }
        }
        Ok(())
    }
}

/// Worker startup identifies its attempt using the existing bearer capability.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CatalogRequest {
    pub run_id: crate::domain::AgentId,
    pub attempt_id: String,
    pub token: String,
}
