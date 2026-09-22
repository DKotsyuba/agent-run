//! Account-scoped first-party quota collection over registered accounts.
//!
//! One global [`AccountId`] owns one physical quota pool per lane across every
//! provider alias, so this driver collects once per `(account, collector)`
//! pair per round regardless of how many labels or providers bind it. Native
//! and named logins stay owned by their harness: only explicit protected
//! stores (environment, file, Keychain) are resolved here, through the same
//! [`CredentialReader`] the custom gateway path uses, and the resolved bytes
//! go straight into the engine-held [`AuthCapability`]; they never enter
//! configuration, diagnostics, or the store. All network work happens outside
//! every database transaction; persistence goes through
//! [`agent_run_store::quota::record_quota_snapshot`] only after a collector
//! round has fully succeeded.

use crate::{
    capacity::lua::{
        run_collector, AllowedOrigin, AuthCapability, AuthPlacement, CollectorError,
        CollectorLimits, CollectorScript, QuotaHttpClient,
    },
    capacity::quota::CollectorScope,
    domain::now,
};
use agent_run_adapters::authorized_request::{CredentialReader, SystemCredentialReader};
use agent_run_domain::{
    catalog::{
        AccountId, AccountStatus, CollectorBinding, LimitsSource, ProviderCatalog, ProviderId,
    },
    CredentialRef, Error, Result,
};
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
    str::FromStr,
    sync::Arc,
};

/// Bounded failure backoff shared across every alias of one account.
///
/// The key is `(global account id, stable collector source)`, so two provider
/// labels over one physical account suppress duplicate remote requests after
/// failures together. The delay grows exponentially from [`BASE_DELAY_SECONDS`]
/// and is capped at [`MAX_DELAY_SECONDS`]; a success clears it. This is a
/// cooperative bound only — it never invents quota facts, and skipped rounds
/// leave the previous samples and durable exhaustion latch untouched.
pub struct AccountBackoff {
    entries: BTreeMap<(String, String), (u32, f64)>,
}

/// First failed-round delay; later rounds double it up to the cap.
const BASE_DELAY_SECONDS: f64 = 60.0;
/// Hard ceiling for one suppressed round, matching the sample TTL horizon.
const MAX_DELAY_SECONDS: f64 = 900.0;
/// Failure count at which the cap is reached; further failures stay there.
const MAX_FAILURES: u32 = 5;

impl Default for AccountBackoff {
    fn default() -> Self {
        Self {
            entries: BTreeMap::new(),
        }
    }
}

impl AccountBackoff {
    /// Returns whether `account`'s `source` round is suppressed at `at`.
    pub fn suppressed(&self, account: &AccountId, source: &str, at: f64) -> bool {
        self.entries
            .get(&(account.as_str().to_owned(), source.to_owned()))
            .is_some_and(|&(_, until)| at < until)
    }

    /// Records one failed round and returns the new suppressed-until epoch.
    pub fn record_failure(&mut self, account: &AccountId, source: &str, at: f64) -> f64 {
        let key = (account.as_str().to_owned(), source.to_owned());
        let failures = self
            .entries
            .get(&key)
            .map_or(0, |(n, _)| *n)
            .saturating_add(1);
        let delay = BASE_DELAY_SECONDS
            * 2_f64
                .powi((failures.min(MAX_FAILURES) - 1) as i32)
                .min(MAX_DELAY_SECONDS / BASE_DELAY_SECONDS) as f64;
        let until = at + delay.min(MAX_DELAY_SECONDS);
        self.entries.insert(key, (failures, until));
        until
    }

    /// Clears any suppression after one successful round.
    pub fn record_success(&mut self, account: &AccountId, source: &str) {
        self.entries
            .remove(&(account.as_str().to_owned(), source.to_owned()));
    }
}

/// One first-party collector: identity, credential placement, and script.
pub struct FirstPartyCollector {
    /// Config spelling of the collector script identity.
    pub id: &'static str,
    /// Stable collector source identity stamped on every emitted window; a
    /// script revision must never change it or pools and latches would fork.
    pub source: &'static str,
    /// Where Rust places the account credential on allowed requests.
    pub placement: AuthPlacement,
    /// Exact Lua source bytes, run inside the bounded engine.
    pub script: &'static str,
}

/// First-party GLM plan-usage collector (official `query-usage` contract).
///
/// `GET {origin}/api/monitor/usage/quota/limit` with the raw `Authorization`
/// token the engine injects (the quota endpoint is not the inference
/// gateway's Bearer style). The payload root is `data` when present, else the
/// top-level object; `limits` entries carry `type` and `percentage`.
/// `TOKENS_LIMIT.percentage` is the five-hour token usage in the official
/// script, reported here as the remaining share of one `primary` pool.
/// `TIME_LIMIT` is monthly MCP usage and does not govern inference quota, so
/// it is deliberately not reported; no weekly or reset fields exist in the
/// official contract and none are invented.
const GLM_QUOTA: FirstPartyCollector = FirstPartyCollector {
    id: "glm_quota",
    source: "glm-quota",
    placement: AuthPlacement::RawAuthorization,
    script: r#"
collect = function(ctx)
  local models = {}
  for name, _ in pairs(ctx.models) do
    models[#models + 1] = name
  end
  local response = ctx.http.request({
    url = ctx.origin .. "/api/monitor/usage/quota/limit"
  })
  if response.status ~= 200 then
    error("quota endpoint status")
  end
  local payload = ctx.json.decode(response.body)
  local data = payload.data
  if data == nil then
    data = payload
  end
  local limits = data.limits
  if limits == nil then
    error("quota payload has no limits")
  end
  local windows = {}
  for _, item in ipairs(limits) do
    if item.type == "TOKENS_LIMIT" and item.percentage ~= nil then
      windows[#windows + 1] = {
        pool = "primary",
        window = "five_hour",
        models = models,
        remaining_percent = 100.0 - item.percentage,
        observed_at = ctx.now
      }
    end
  end
  if #windows == 0 then
    error("quota payload has no token limit")
  end
  return { version = 1, windows = windows }
end
"#,
};

/// First-party Anthropic OAuth usage collector.
///
/// `GET {origin}/api/oauth/usage` with the engine-injected Bearer token and
/// the `anthropic-beta: oauth-2025-04-20` header the endpoint requires. The
/// normalizer ports the recorded envelope (`limits[]` of `kind`/`percent`,
/// optional RFC 3339 `resets_at`): `session` maps to the `primary` lane's
/// `five_hour` window and weekly kinds to `secondary`/`seven_day`. That
/// envelope is the only recorded observation shape; it has **not** been
/// re-verified against a live endpoint, so live rounds may still fail with
/// the engine's typed categories rather than fabricate windows.
const ANTHROPIC_USAGE: FirstPartyCollector = FirstPartyCollector {
    id: "anthropic_usage",
    source: "anthropic-usage",
    placement: AuthPlacement::BearerAuthorization,
    script: r#"
collect = function(ctx)
  local models = {}
  for name, _ in pairs(ctx.models) do
    models[#models + 1] = name
  end
  local response = ctx.http.request({
    url = ctx.origin .. "/api/oauth/usage",
    headers = { ["anthropic-beta"] = "oauth-2025-04-20" }
  })
  if response.status ~= 200 then
    error("usage endpoint status")
  end
  local payload = ctx.json.decode(response.body)
  local limits = payload.limits
  if limits == nil then
    error("usage payload has no limits")
  end
  local windows = {}
  for _, entry in ipairs(limits) do
    local lane = "secondary"
    local window = "seven_day"
    if entry.kind == "session" then
      lane = "primary"
      window = "five_hour"
    end
    local reset_at = nil
    if entry.resets_at ~= nil then
      reset_at = ctx.time.rfc3339(entry.resets_at)
    end
    windows[#windows + 1] = {
      pool = lane,
      window = window,
      models = models,
      remaining_percent = 100.0 - entry.percent,
      reset_at = reset_at,
      observed_at = ctx.now
    }
  end
  if #windows == 0 then
    error("usage payload has no limits")
  end
  return { version = 1, windows = windows }
end
"#,
};

/// Returns the first-party collector bound by `binding.script`, if known.
///
/// Unknown identities return `None`; callers report the typed
/// `collector_unknown` failure rather than guessing from a provider name.
pub fn first_party(script: &str) -> Option<&'static FirstPartyCollector> {
    match script {
        "glm_quota" => Some(&GLM_QUOTA),
        "anthropic_usage" => Some(&ANTHROPIC_USAGE),
        _ => None,
    }
}

/// One deduplicated collection unit for a round.
struct Planned {
    /// The collector this unit runs; `None` marks an explicitly configured
    /// script identity no first-party collector answers to.
    collector: Option<&'static FirstPartyCollector>,
    /// The configured script identity spelling, for reports.
    script: String,
    /// Explicit binding the plan's first (deterministic) provider declared.
    binding: CollectorBinding,
    /// Union of `native_model.unwrap_or(id)` across every bound provider.
    models: BTreeMap<String, Value>,
    /// Persistence runtime scope: the first bound provider id in order.
    runtime: ProviderId,
    /// The single global account whose physical pools this unit observes.
    account: AccountId,
}

/// Builds the deduplicated per-`(account, script)` plan for one catalog.
///
/// Providers without a Lua limits source are skipped; the same global account
/// bound under several aliases collapses into one unit whose model set is the
/// union of each provider's native model spellings, so one physical account
/// yields one remote request set and one pool family. An unknown script
/// identity stays in the plan so its round reports the typed
/// `collector_unknown` failure instead of being silently dropped.
fn plan_catalog(catalog: &ProviderCatalog) -> Result<Vec<Planned>> {
    let mut units: BTreeMap<(String, String), Planned> = BTreeMap::new();
    for provider in catalog.providers() {
        let binding = match (&provider.limits_source, &provider.collector) {
            (LimitsSource::Lua, Some(binding)) => binding,
            (LimitsSource::Lua, None) => {
                return Err(Error::Validation(
                    "lua limits source requires a collector binding".into(),
                ));
            }
            _ => continue,
        };
        for model in &provider.models {
            let native = model
                .native_model
                .clone()
                .unwrap_or_else(|| model.id.clone());
            let value = json!({"id": model.id, "native_model": native});
            for bound in &provider.bindings {
                units
                    .entry((bound.account.as_str().to_owned(), binding.script.clone()))
                    .or_insert_with(|| Planned {
                        collector: first_party(&binding.script),
                        script: binding.script.clone(),
                        binding: binding.clone(),
                        models: BTreeMap::new(),
                        runtime: provider.id.clone(),
                        account: bound.account.clone(),
                    })
                    .models
                    .insert(native.clone(), value.clone());
            }
        }
    }
    Ok(units.into_values().collect())
}

/// Resolves one account's credential into an engine-held capability.
///
/// Native and named harness logins are refused: they stay owned by their
/// harness and never become exportable quota tokens. Explicit protected
/// stores are read through `reader` at request time only; the value lands in
/// the redacted [`AuthCapability`] and nowhere else.
fn capability(
    account: &AccountId,
    record: &agent_run_domain::catalog::AccountRecord,
    binding: &CollectorBinding,
    placement: &AuthPlacement,
    reader: &impl CredentialReader,
) -> Result<AuthCapability> {
    let reference = CredentialRef::from_str(record.secret_ref.as_str())?;
    let value = reader.read(&reference)?;
    let origins = binding
        .origins
        .iter()
        .map(|origin| {
            AllowedOrigin::canonical(origin)
                .and_then(|(scheme, host, port)| AllowedOrigin::new(&scheme, &host, port).ok())
        })
        .collect::<Option<Vec<_>>>()
        .ok_or_else(|| crate::error::invalid("invalid collector origin"))?;
    Ok(AuthCapability::new(
        account.clone(),
        placement.clone(),
        value.into(),
        origins,
    )?)
}

/// Maps one typed collector failure to its static round issue code.
fn issue_of(error: &CollectorError) -> String {
    error.to_string()
}

/// Runs one account-scoped quota collection round for a whole catalog.
///
/// For every deduplicated `(account, collector)` unit whose account is
/// registered and enabled and not currently suppressed by `backoff`, resolves
/// the protected credential, runs the first-party script in the bounded Lua
/// engine against `client`, and persists the normalized snapshot through the
/// quota store with `retention`. Network requests and credential reads happen
/// strictly before each store transaction is opened. Every failure is a
/// static typed code in the returned report; no provider body, credential
/// bytes, or secret reference ever appears there.
pub async fn collect_provider_quota(
    home: &Path,
    catalog: &ProviderCatalog,
    retention: usize,
    limits: &CollectorLimits,
    client: Arc<dyn QuotaHttpClient>,
    reader: &impl CredentialReader,
    backoff: &mut AccountBackoff,
) -> Result<Value> {
    let mut results = Vec::new();
    let mut all_ok = true;
    for unit in plan_catalog(catalog)? {
        let source = unit
            .collector
            .map(|collector| collector.source)
            .unwrap_or("unknown");
        let entry = json!({
            "account": unit.account.as_str(),
            "source": source,
            "runtime": unit.runtime.as_str(),
        });
        let record = catalog.account(&unit.account);
        let mut issues = Vec::new();
        let mut windows = 0usize;
        let at = now();
        let Some(collector) = unit.collector else {
            issues.push("collector_unknown".to_owned());
            all_ok = false;
            results.push(finish(entry, "failed", 0, issues));
            continue;
        };
        if record.is_none_or(|record| record.status != AccountStatus::Enabled) {
            issues.push("account_disabled".to_owned());
        } else if backoff.suppressed(&unit.account, collector.source, at) {
            issues.push("backoff".to_owned());
        } else {
            let record = record.expect("enabled record");
            match capability(
                &unit.account,
                record,
                &unit.binding,
                &collector.placement,
                reader,
            ) {
                Ok(auth) => {
                    let origin = unit
                        .binding
                        .origins
                        .first()
                        .expect("validated binding has an origin");
                    let script = CollectorScript::new(collector.script);
                    let scope = CollectorScope {
                        runtime: unit.runtime.as_str().to_owned(),
                        source: collector.source.to_owned(),
                        models: unit.models.keys().cloned().collect(),
                    };
                    match run_collector(
                        &script,
                        &scope,
                        &unit.account,
                        &unit.models,
                        at,
                        limits,
                        client.clone(),
                        &auth,
                        origin,
                    )
                    .await
                    {
                        Ok(snapshot) => {
                            // All network work is complete before the store
                            // opens its persistence transaction.
                            match agent_run_store::quota::record_quota_snapshot(
                                home,
                                unit.runtime.as_str(),
                                &snapshot,
                                retention,
                                now(),
                            ) {
                                Ok(_) => {
                                    windows = snapshot
                                        .models
                                        .first()
                                        .map(|model| model.pools.len())
                                        .unwrap_or(0);
                                    backoff.record_success(&unit.account, collector.source);
                                }
                                Err(error) => issues.push(static_reason(&error)),
                            }
                        }
                        Err(error) => {
                            backoff.record_failure(&unit.account, collector.source, at);
                            issues.push(issue_of(&error));
                        }
                    }
                }
                Err(error) => {
                    backoff.record_failure(&unit.account, collector.source, at);
                    issues.push(static_reason(&error));
                }
            }
        }
        let status = if issues.is_empty() && windows > 0 {
            "collected"
        } else if issues.is_empty() {
            "no_data"
        } else {
            all_ok = false;
            "failed"
        };
        results.push(finish(entry, status, windows, issues));
    }
    Ok(json!({"ok": all_ok, "results": results}))
}

/// Stamps one unit's report row with its outcome facts.
fn finish(mut entry: Value, status: &str, windows: usize, issues: Vec<String>) -> Value {
    entry["status"] = json!(status);
    entry["windows"] = json!(windows);
    entry["issues"] = json!(issues);
    entry
}

/// Reduces one domain error to its static reason text.
///
/// Domain reasons are already secret-free fixed codes; anything else becomes
/// a generic bucket so no incidental text can reach the report.
fn static_reason(error: &Error) -> String {
    match error {
        Error::Validation(reason) => reason.clone(),
        Error::Runtime(reason) => format!("runtime:{reason}"),
        _ => "source_failed".to_owned(),
    }
}

/// Convenience wrapper using the host protected-store reader and the shared
/// reqwest transport; the polling loop owns `backoff` across rounds.
pub async fn collect(home: &Path, catalog: &ProviderCatalog, retention: usize) -> Result<Value> {
    let limits = CollectorLimits::default();
    let client = Arc::new(
        crate::capacity::lua::ReqwestQuotaHttp::new(limits.http_response_body_bytes)
            .map_err(|_| Error::Runtime("quota transport unavailable".into()))?,
    );
    let mut backoff = AccountBackoff::default();
    collect_provider_quota(
        home,
        catalog,
        retention,
        &limits,
        client,
        &SystemCredentialReader,
        &mut backoff,
    )
    .await
}

/// Returns every distinct account/script pair a catalog would collect.
///
/// Read-only helper for tests and diagnostics; performs no network or store
/// access.
pub fn planned_pairs(catalog: &ProviderCatalog) -> Result<BTreeSet<(String, String)>> {
    Ok(plan_catalog(catalog)?
        .into_iter()
        .map(|unit| (unit.account.as_str().to_owned(), unit.script))
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Unknown collector identities stay unknown instead of guessing.
    #[test]
    fn unknown_scripts_are_not_resolved() {
        assert!(first_party("codex_appserver").is_none());
        assert!(first_party("glm_quota").is_some_and(|c| c.source == "glm-quota"));
        assert!(first_party("anthropic_usage").is_some_and(|c| c.source == "anthropic-usage"));
    }

    /// Backoff grows boundedly and is shared by its account/source key.
    #[test]
    fn backoff_is_bounded_and_keyed_by_account() {
        let mut backoff = AccountBackoff::default();
        let account: AccountId = "acct-a".parse().unwrap();
        assert!(!backoff.suppressed(&account, "glm-quota", 0.0));
        let until = backoff.record_failure(&account, "glm-quota", 100.0);
        assert_eq!(until, 160.0);
        assert!(backoff.suppressed(&account, "glm-quota", 159.0));
        assert!(!backoff.suppressed(&account, "glm-quota", 160.0));
        // A distinct collector source on the same account is not suppressed.
        assert!(!backoff.suppressed(&account, "anthropic-usage", 100.0));
        for _ in 0..10 {
            backoff.record_failure(&account, "glm-quota", 100.0);
        }
        let until = backoff.record_failure(&account, "glm-quota", 100.0);
        assert_eq!(until, 1000.0, "capped at the sample TTL horizon");
        backoff.record_success(&account, "glm-quota");
        assert!(!backoff.suppressed(&account, "glm-quota", 100.0));
    }
}
