//! Local provider selection and compatibility views over independent Quota results.

pub mod provider_catalog;
pub mod provider_ranking;
/// Adapts independent Quota snapshots to local candidate selection.
pub mod quota_adapter;
/// Read projections over independent Quota results and local eligibility.
pub mod quota_projection;
/// Bounded native metadata probes for legacy model rosters.
pub mod sources;

/// Reads the legacy schema-1 native model roster without collecting quota.
pub async fn models(home: &std::path::Path) -> crate::Result<serde_json::Value> {
    sources::models(home).await
}
