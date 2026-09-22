//! Deterministic, read-only planning for one-time v1-to-v2 configuration migration.
//!
//! This module never applies a config, edits a database, or guesses an account
//! or model alias. A later coordinated consumer owns backup and publication.

use crate::{
    config::{Adapter, Config},
    provider_config::{HarnessConfig, ProviderConfig, ProviderSettings},
};
use agent_run_domain::{
    catalog::{
        AccountId, AccountRecord, AuthFamily, HarnessId, LegacyRuntime, LimitsSource,
        ProviderBinding, ProviderConnection, ProviderId, ProviderModel,
    },
    domain::Constraint,
    error::invalid,
    types::PositiveFinite,
    Result,
};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
};

/// Operator-supplied mapping for one historical runtime name.
///
/// Every v1 model requires an explicit native alias, including identity
/// aliases. Every old account label and the omitted/global selector require
/// explicit global account ids. No credential bytes appear here.
#[derive(Debug, Clone)]
pub struct RuntimeMapping {
    /// New arbitrary provider id, never inferred from the v1 runtime name.
    pub provider: ProviderId,
    /// Codex or Claude Code harness chosen explicitly.
    pub harness: HarnessId,
    /// Native login or configured custom gateway.
    pub connection: ProviderConnection,
    /// Exact credential family of all mapped accounts.
    pub auth_family: AuthFamily,
    /// New v2 source: codex_appserver, lua, or none.
    pub limits_source: LimitsSource,
    /// One native model id per historical public model id.
    pub native_models: BTreeMap<String, String>,
    /// Model-specific hard constraints to carry into v2.
    pub model_restrictions: BTreeMap<String, Vec<Constraint>>,
    /// Account used when the old request omitted its account label.
    pub global_account: AccountId,
    /// Global ids for every declared v1 account label.
    pub labelled_accounts: BTreeMap<String, AccountId>,
}

/// Read-only migration output for review and later coordinated publication.
#[derive(Debug, Clone)]
pub struct MigrationPlan {
    /// Fully validated proposed v2 configuration, never written by this API.
    pub config: ProviderConfig,
    /// Recorded adapter evidence for historical request decoding.
    pub legacy_runtime_map: BTreeMap<String, LegacyRuntime>,
    /// Stable runtime names requiring manual native-auth or harness review.
    pub manual_review: Vec<String>,
}

/// Plans an explicit mapping of every v1 runtime without touching durable
/// state. `harnesses` and `mappings` are operator-owned; `accounts` come
/// from the global registry. Missing or extra mappings, unrepresented model
/// aliases/accounts, incompatible harnesses, and untranslatable lane weights
/// fail instead of guessing. The result retains old runtime spellings for
/// `decode_legacy_request` and reports nonsecret manual-review markers.
pub fn plan_v1(
    old: &Config,
    harnesses: BTreeMap<HarnessId, HarnessConfig>,
    mappings: BTreeMap<String, RuntimeMapping>,
    accounts: Vec<AccountRecord>,
    home: &Path,
) -> Result<MigrationPlan> {
    let mut old = old.clone();
    old.validate(home)?;
    if old.schema_version != 1
        || old.runtimes.keys().collect::<Vec<_>>() != mappings.keys().collect::<Vec<_>>()
    {
        return Err(invalid(
            "every v1 runtime needs exactly one explicit migration mapping",
        ));
    }
    let mut providers = BTreeMap::new();
    let mut legacy_runtime_map = BTreeMap::new();
    let mut manual_review = Vec::new();
    for (runtime_name, runtime) in &old.runtimes {
        if !runtime.enabled {
            return Err(invalid(
                "disabled v1 runtimes need explicit removal before migration",
            ));
        }
        let mapped = &mappings[runtime_name];
        let expected = match runtime.kind()? {
            Adapter::Codex => HarnessId::Codex,
            Adapter::Claude | Adapter::Glm => HarnessId::ClaudeCode,
        };
        if mapped.harness != expected || !harnesses.contains_key(&expected) {
            return Err(invalid("v1 adapter and mapped harness disagree"));
        }
        if runtime.kind()? == Adapter::Glm
            && !matches!(mapped.connection, ProviderConnection::Custom { .. })
        {
            return Err(invalid(
                "GLM migration requires an explicit Claude Messages gateway",
            ));
        }
        let old_models: BTreeSet<_> = runtime.models.iter().cloned().collect();
        if old_models != mapped.native_models.keys().cloned().collect()
            || !mapped
                .model_restrictions
                .keys()
                .all(|model| old_models.contains(model))
        {
            return Err(invalid(
                "every historical model needs one explicit native alias",
            ));
        }
        let old_labels: BTreeSet<_> = runtime.accounts.iter().map(String::as_str).collect();
        if old_labels.contains("global")
            || old_labels
                != mapped
                    .labelled_accounts
                    .keys()
                    .map(String::as_str)
                    .collect()
            || runtime
                .priority_account_multipliers
                .keys()
                .any(|label| !old_labels.contains(label.as_str()))
            || !runtime.priority_lane_multipliers.is_empty()
        {
            return Err(invalid(
                "legacy account labels or lane weights need explicit resolution",
            ));
        }
        let provider_weight = PositiveFinite::try_from(runtime.priority_multiplier)?;
        let mut bindings = vec![ProviderBinding {
            label: "global".parse()?,
            account: mapped.global_account.clone(),
            models: None,
            multiplier: PositiveFinite::try_from(1.0)?,
        }];
        for (label, account) in &mapped.labelled_accounts {
            let weight = runtime
                .priority_account_multipliers
                .get(label.as_str())
                .copied()
                .unwrap_or(runtime.priority_multiplier);
            bindings.push(ProviderBinding {
                label: label.parse()?,
                account: account.clone(),
                models: None,
                multiplier: PositiveFinite::try_from(weight / runtime.priority_multiplier)?,
            });
        }
        let models = runtime
            .models
            .iter()
            .map(|id| ProviderModel {
                id: id.clone(),
                native_model: mapped.native_models.get(id).cloned(),
                params: BTreeMap::new(),
                allowed_params: BTreeMap::new(),
                recommendations: vec![],
                restrictions: mapped
                    .model_restrictions
                    .get(id)
                    .cloned()
                    .unwrap_or_default(),
            })
            .collect();
        let settings = ProviderSettings {
            harness: mapped.harness,
            connection: mapped.connection.clone(),
            auth_family: mapped.auth_family.clone(),
            models,
            bindings,
            recommendations: vec![],
            priority_multiplier: provider_weight,
            limits_source: mapped.limits_source,
        };
        if providers
            .insert(mapped.provider.clone(), settings)
            .is_some()
        {
            return Err(invalid("migration provider ids must be unique"));
        }
        legacy_runtime_map.insert(
            runtime_name.clone(),
            LegacyRuntime {
                provider: mapped.provider.clone(),
                harness: mapped.harness,
            },
        );
        if runtime.auth.is_some() {
            manual_review.push(format!("{runtime_name}:auth_reference"));
        }
        if let Some(harness) = harnesses.get(&mapped.harness) {
            if harness.binary != runtime.binary
                || harness.home != runtime.home
                || harness.native_settings != runtime.native_settings
                || harness.workspace_roots != runtime.workspace_roots
                || harness.workspace_network != runtime.workspace_network
                || harness.max_active_agents != runtime.max_active_agents
                || harness.plugins != runtime.plugins
                || harness.plugin_snapshot_assets != runtime.plugin_snapshot_assets
                || harness.environment != runtime.environment
                || serde_json::to_value(&harness.hooks)? != serde_json::to_value(&runtime.hooks)?
                || serde_json::to_value(&harness.rust)? != serde_json::to_value(&runtime.rust)?
            {
                manual_review.push(format!("{runtime_name}:harness_settings"));
            }
        }
    }
    let mut config = ProviderConfig {
        schema_version: 2,
        core: old.core.clone(),
        capacity: old.capacity.clone(),
        delivery: old.delivery.clone(),
        profiles: old.profiles.clone(),
        skills: old.skills.clone(),
        mcp: old.mcp.clone(),
        environments: old.environments.clone(),
        harnesses,
        providers,
    };
    config.validate(home)?;
    config.resolve_catalog(accounts)?;
    Ok(MigrationPlan {
        config,
        legacy_runtime_map,
        manual_review,
    })
}
