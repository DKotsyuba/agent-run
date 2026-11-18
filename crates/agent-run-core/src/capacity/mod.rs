//! Local provider selection over independent Quota results.
//!
//! Run keeps only the minimal Quota read admission needs (fetch, candidate
//! scoring and the `QuotaExhausted` refusal); Agent Quota owns quota and
//! capacity presentation.

pub mod provider_catalog;
pub mod provider_ranking;
/// Adapts independent Quota snapshots to local candidate selection.
pub mod quota_adapter;
/// Bounded native metadata probes (legacy schema-1 helpers).
pub mod sources;
