//! Account-scoped Codex quota through the app-server metadata probe.
//!
//! One registered global account is probed once per round no matter how many
//! provider aliases bind it. The probe home is chosen by the account's own
//! protected reference — `native:codex` links the host login, `named:codex:*`
//! links that label's account home — never by a provider's display label, and
//! the verified cleanup contract of
//! [`crate::capacity::sources::codex_probe`] is reused unchanged.

use super::collectors::{finish, row_status, window_count, AccountBackoff};
use super::quota::{normalize_collector_output, CollectorScope};
use super::quota_auth::{CREDENTIAL_UNAVAILABLE, STORE_FAILED};
use crate::domain::now;
use agent_run_config::provider_config::ProviderConfig;
use agent_run_domain::{
    catalog::{
        AccountId, AccountStatus, HarnessId, LimitsSource, NormalizedQuotaSnapshot,
        ProviderCatalog, ProviderId,
    },
    CredentialRef, Result,
};
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
    str::FromStr,
};

/// Stable collector source identity for Codex app-server observations.
pub const CODEX_SOURCE: &str = "codex-appserver";

/// The app-server bucket id of the general Codex lane.
const GENERAL_LIMIT: &str = "codex";

/// Returns the lowercase alphanumeric token sequence of one lane or model name.
fn tokens(text: &str) -> Vec<String> {
    text.split(|c: char| !c.is_ascii_alphanumeric())
        .filter(|token| !token.is_empty())
        .map(str::to_ascii_lowercase)
        .collect()
}

/// Maps one app-server `account/rateLimits/read` response into the validated
/// version-1 collector shape and normalizes it for `account`.
///
/// Accepts the current `rateLimitsByLimitId` map and the legacy single
/// `rateLimits` bucket (keyed by its `limitId`, else `codex`), exactly as
/// `normalize_codex` does. Lane membership uses only the provider's own
/// naming: a bucket whose `limitName` token sequence equals a bound model's
/// native spelling (`GPT-5.3-Codex-Spark` ↔ `gpt-5.3-codex-spark`) governs
/// that model alone; the general `codex` bucket governs every other bound
/// model; any other bucket has no known membership and is skipped rather
/// than applied to unrelated models. Returns the snapshot and the number of
/// skipped buckets. Malformed present windows fail the whole response.
pub fn codex_snapshot(
    account: &AccountId,
    runtime: &str,
    models: &BTreeSet<String>,
    response: &Value,
    observed: f64,
) -> Result<(NormalizedQuotaSnapshot, usize)> {
    let raw = response
        .get("result")
        .filter(|value| value.is_object())
        .unwrap_or(response);
    let buckets = if let Some(map) = raw.get("rateLimitsByLimitId").and_then(Value::as_object) {
        map.clone()
    } else if let Some(bucket) = raw.get("rateLimits").filter(|value| value.is_object()) {
        let id = bucket
            .get("limitId")
            .and_then(Value::as_str)
            .filter(|id| !id.is_empty())
            .unwrap_or(GENERAL_LIMIT);
        serde_json::Map::from_iter([(id.to_owned(), bucket.clone())])
    } else {
        return Err(crate::error::invalid(
            "codex quota response has no bucket map",
        ));
    };
    // Dedicated lanes first, so the general lane covers only the remainder.
    let mut members: BTreeMap<String, Vec<&String>> = BTreeMap::new();
    for (id, bucket) in &buckets {
        let name = bucket
            .get("limitName")
            .and_then(Value::as_str)
            .unwrap_or(id);
        let named: Vec<&String> = models
            .iter()
            .filter(|model| tokens(model) == tokens(name))
            .collect();
        if id != GENERAL_LIMIT && !named.is_empty() {
            members.insert(id.clone(), named);
        }
    }
    let dedicated: BTreeSet<&String> = members.values().flatten().copied().collect();
    if buckets.contains_key(GENERAL_LIMIT) {
        let general: Vec<&String> = models
            .iter()
            .filter(|model| !dedicated.contains(model))
            .collect();
        if !general.is_empty() {
            members.insert(GENERAL_LIMIT.to_owned(), general);
        }
    }
    let skipped = buckets.len() - members.len();
    let mut windows = Vec::new();
    for (limit_id, bucket) in &buckets {
        let Some(bound) = members.get(limit_id) else {
            continue;
        };
        for field in ["primary", "secondary"] {
            let Some(value) = bucket.get(field).filter(|value| !value.is_null()) else {
                continue;
            };
            let used = value
                .get("usedPercent")
                .and_then(Value::as_f64)
                .filter(|used| (0.0..=100.0).contains(used))
                .ok_or_else(|| crate::error::invalid("codex quota window is malformed"))?;
            let minutes = value
                .get("windowDurationMins")
                .and_then(Value::as_f64)
                .filter(|minutes| *minutes > 0.0)
                .ok_or_else(|| crate::error::invalid("codex quota window is malformed"))?;
            let reset_at = match value.get("resetsAt").filter(|reset| !reset.is_null()) {
                None => None,
                Some(reset) => Some(
                    reset
                        .as_f64()
                        .filter(|reset| (0.0..=253_402_300_799.0).contains(reset))
                        .ok_or_else(|| crate::error::invalid("codex quota window is malformed"))?,
                ),
            };
            windows.push(json!({
                "pool": limit_id,
                "window": crate::capacity::sources::window_name(minutes),
                "models": bound,
                "remaining_percent": 100.0 - used,
                "reset_at": reset_at,
                "observed_at": observed,
            }));
        }
    }
    if windows.is_empty() {
        return Err(crate::error::invalid("codex quota response has no windows"));
    }
    let scope = CollectorScope {
        runtime: runtime.to_owned(),
        source: CODEX_SOURCE.to_owned(),
        models: models.clone(),
    };
    let snapshot = normalize_collector_output(
        account,
        &scope,
        &json!({"version": 1, "windows": windows}),
        observed,
        256,
        256,
    )?;
    Ok((snapshot, skipped))
}

/// One deduplicated physical Codex account for a round.
struct CodexUnit {
    /// Persistence runtime scope: the first bound provider id in order.
    runtime: ProviderId,
    /// Union of every enabled binding's effective native model subset.
    models: BTreeSet<String>,
}

/// Groups every CodexAppserver binding by its global account.
///
/// Each binding contributes its own model subset (or the provider's full
/// set when it inherits), keyed by native spelling.
fn plan_codex(catalog: &ProviderCatalog) -> BTreeMap<AccountId, CodexUnit> {
    let mut units: BTreeMap<AccountId, CodexUnit> = BTreeMap::new();
    for provider in catalog.providers() {
        if provider.limits_source != LimitsSource::CodexAppserver {
            continue;
        }
        for bound in &provider.bindings {
            let unit = units.entry(bound.account.clone()).or_insert(CodexUnit {
                runtime: provider.id.clone(),
                models: BTreeSet::new(),
            });
            unit.models.extend(
                provider
                    .models
                    .iter()
                    .filter(|model| {
                        bound
                            .models
                            .as_ref()
                            .is_none_or(|subset| subset.contains(&model.id))
                    })
                    .map(|model| {
                        model
                            .native_model
                            .clone()
                            .unwrap_or_else(|| model.id.clone())
                    }),
            );
        }
    }
    units
}

/// One app-server round for every CodexAppserver account in a v2 catalog.
///
/// Probing is network work and completes before any store transaction
/// opens. Each report row carries only static typed codes; a response whose
/// buckets have no known model membership reports `unmapped_lanes`.
pub async fn codex_appserver_round(
    home: &Path,
    config: &ProviderConfig,
    catalog: &ProviderCatalog,
    backoff: &mut AccountBackoff,
    retention: usize,
) -> Result<Vec<Value>> {
    let mut results = Vec::new();
    let units = plan_codex(catalog);
    if units.is_empty() {
        return Ok(results);
    }
    let harness = config
        .harnesses
        .get(&HarnessId::Codex)
        .ok_or_else(|| crate::error::invalid("codex quota requires the codex harness"))?;
    // The probe environment reads only the shared environment declarations;
    // a synthesized legacy view supplies exactly that plus the harness paths.
    let legacy = crate::config::Config {
        schema_version: 1,
        core: config.core.clone(),
        capacity: config.capacity.clone(),
        delivery: config.delivery.clone(),
        profiles: config.profiles.clone(),
        skills: config.skills.clone(),
        mcp: config.mcp.clone(),
        environments: config.environments.clone(),
        runtimes: BTreeMap::new(),
    };
    for (account, unit) in units {
        let at = now();
        let mut issues = Vec::new();
        let mut windows = 0usize;
        let mut unmapped = 0usize;
        let record = catalog.account(&account);
        let label = record.map(|record| CredentialRef::from_str(record.secret_ref.as_str()));
        if record.is_none_or(|record| record.status != AccountStatus::Enabled) {
            issues.push("account_disabled".to_owned());
        } else if unit.models.is_empty() {
            issues.push("no_bound_models".to_owned());
        } else if backoff.suppressed(&account, CODEX_SOURCE, at) {
            issues.push("backoff".to_owned());
        } else {
            let selected = match label {
                Some(Ok(CredentialRef::Native(HarnessId::Codex))) => Some(None),
                Some(Ok(CredentialRef::Named {
                    harness: HarnessId::Codex,
                    label,
                })) => Some(Some(label.as_str().to_owned())),
                _ => None,
            };
            match selected {
                None => issues.push(CREDENTIAL_UNAVAILABLE.to_owned()),
                Some(selected) => {
                    let runtime = serde_json::from_value::<crate::config::Runtime>(json!({
                        "enabled": true,
                        "adapter": "codex",
                        "binary": harness.binary,
                        "home": harness.home,
                        "models": unit.models,
                    }))
                    .map_err(|_| crate::error::invalid("invalid synthesized codex runtime"))?;
                    let observed = now();
                    let outcome = crate::capacity::sources::codex_probe(
                        home,
                        &legacy,
                        &runtime,
                        selected.as_deref(),
                        "account/rateLimits/read",
                    )
                    .await
                    .and_then(|value| {
                        codex_snapshot(
                            &account,
                            unit.runtime.as_str(),
                            &unit.models,
                            &value,
                            observed,
                        )
                    });
                    match outcome {
                        Ok((snapshot, skipped)) => {
                            unmapped = skipped;
                            match agent_run_store::quota::record_quota_snapshot(
                                home,
                                unit.runtime.as_str(),
                                &snapshot,
                                retention,
                                now(),
                            ) {
                                Ok(_) => {
                                    windows = window_count(&snapshot);
                                    backoff.record_success(&account, CODEX_SOURCE);
                                }
                                Err(_) => issues.push(STORE_FAILED.to_owned()),
                            }
                        }
                        Err(_) => {
                            backoff.record_failure(&account, CODEX_SOURCE, at);
                            issues.push("probe_failed".to_owned());
                        }
                    }
                }
            }
        }
        let status = row_status(&issues, windows);
        let mut row = finish(
            json!({
                "account": account.as_str(),
                "source": CODEX_SOURCE,
                "runtime": unit.runtime.as_str(),
            }),
            status,
            windows,
            issues,
        );
        row["unmapped_lanes"] = json!(unmapped);
        results.push(row);
    }
    Ok(results)
}
