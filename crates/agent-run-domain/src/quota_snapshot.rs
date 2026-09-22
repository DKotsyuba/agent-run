//! Normalized, host-bound quota observations passed from collectors to scoring.

use crate::{
    catalog::{AccountId, PhysicalQuotaKey},
    domain::nonblank,
    error::invalid,
    Result,
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

/// One physical quota window. Unknown remaining or reset data stays absent.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QuotaWindow {
    /// Collector source identity, bounded to 128 bytes.
    pub source: String,
    /// Provider-native window name, bounded to 128 bytes.
    pub name: String,
    /// Remaining percentage in 0..=100, or unknown.
    pub remaining_percent: Option<f64>,
    /// Provider-reported Unix reset time in seconds, or unknown.
    pub reset_at: Option<f64>,
    /// Unix seconds when the collector observed this window.
    pub observed_at: f64,
    /// Unix seconds after which this observation is stale.
    pub valid_until: f64,
}

impl QuotaWindow {
    /// Rejects invented or invalid numeric facts and inverted observation time.
    pub fn validate(&self) -> Result<()> {
        nonblank("quota source", &self.source)?;
        nonblank("quota window", &self.name)?;
        if self.source.len() > 128
            || self.name.len() > 128
            || self
                .remaining_percent
                .is_some_and(|n| !n.is_finite() || !(0.0..=100.0).contains(&n))
            || self.reset_at.is_some_and(|n| !n.is_finite())
            || !self.observed_at.is_finite()
            || !self.valid_until.is_finite()
            || self.valid_until < self.observed_at
        {
            return Err(invalid("invalid quota window"));
        }
        Ok(())
    }
}

/// One physical pool and its directly attributable provider windows.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QuotaPoolObservation {
    /// Global account plus provider-independent quota lane.
    pub key: PhysicalQuotaKey,
    /// Provider-reported windows; empty means this pool's capacity is unknown.
    pub windows: Vec<QuotaWindow>,
}

/// Explicit model membership and every physical pool it consumes.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QuotaModelObservation {
    /// Catalog model id, never inferred by the collector.
    pub model: String,
    /// Nonempty physical pools consumed together by this model.
    pub pools: Vec<QuotaPoolObservation>,
}

/// One account-scoped collector handoff. No scoring or reservation lives here.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NormalizedQuotaSnapshot {
    /// Validated host account that owns every physical key.
    pub account: AccountId,
    /// Explicit model memberships, bounded to 256 entries.
    pub models: Vec<QuotaModelObservation>,
}

impl NormalizedQuotaSnapshot {
    /// Checks account ownership, unique model/key/window identities, bounded
    /// facts, and identical order-independent windows for shared physical
    /// pools before a quota producer scores the snapshot.
    pub fn validate(&self) -> Result<()> {
        if self.models.len() > 256 {
            return Err(invalid("too many quota models"));
        }
        let mut models = BTreeSet::new();
        let mut physical_windows = BTreeMap::new();
        for model in &self.models {
            nonblank("quota model", &model.model)?;
            if model.model.len() > 256
                || !models.insert(&model.model)
                || model.pools.is_empty()
                || model.pools.len() > 32
            {
                return Err(invalid("invalid quota model membership"));
            }
            let mut keys = BTreeSet::new();
            for pool in &model.pools {
                if !pool.key.belongs_to(&self.account) || !keys.insert(&pool.key) {
                    return Err(invalid("quota key belongs to another account or repeats"));
                }
                if pool.windows.len() > 32 {
                    return Err(invalid("too many quota windows"));
                }
                let mut windows = BTreeSet::new();
                for window in &pool.windows {
                    window.validate()?;
                    if !windows.insert((&window.source, &window.name)) {
                        return Err(invalid("quota window repeats"));
                    }
                }
                let mut canonical = pool.windows.clone();
                canonical.sort_by(|a, b| (&a.source, &a.name).cmp(&(&b.source, &b.name)));
                if physical_windows
                    .insert(pool.key.clone(), canonical.clone())
                    .is_some_and(|previous| previous != canonical)
                {
                    return Err(invalid("shared quota pool observations disagree"));
                }
            }
        }
        Ok(())
    }
}
