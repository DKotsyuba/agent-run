//! Account-scoped external quota collection with alias deduplication and durable backoff.
//! All provider protocol and output conversion live in configured executable files.

pub use super::quota_auth::QuotaCredentialReader;
use super::quota_auth::{BACKOFF_PERSIST_FAILED, CREDENTIAL_UNAVAILABLE, STORE_FAILED};
use super::{
    executable,
    quota::{normalize_collector_output, CollectorScope, MAX_OUTPUT_MODELS, MAX_OUTPUT_WINDOWS},
};
use crate::domain::now;
use agent_run_adapters::authorized_request::CredentialReader;
use agent_run_config::provider_config::ProviderConfig;
use agent_run_domain::{
    catalog::{
        AccountId, AccountStatus, CollectorBinding, HarnessId, LimitsSource, ProviderCatalog,
        ProviderId,
    },
    CredentialRef, Result,
};
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
};

/// Bounded failure backoff shared across every alias of one account.
///
/// The key is `(global account id, stable collector source)`, so two provider
/// labels over one physical account suppress duplicate remote requests after
/// failures together. The delay grows exponentially from [`BASE_DELAY_SECONDS`]
/// and is capped at [`MAX_DELAY_SECONDS`] or an endpoint-declared
/// `retry-after` horizon; a success clears it. State is durable in
/// `capacity/backoff.json` under the agent-run home so suppression survives
/// the process boundary between polling rounds. This is a cooperative bound
/// only — it never invents quota facts, and skipped rounds leave the previous
/// samples and durable exhaustion latch untouched.
#[derive(Default)]
pub struct AccountBackoff {
    entries: BTreeMap<(String, String), (u32, f64)>,
}

/// First failed-round delay; later rounds double it up to the cap.
const BASE_DELAY_SECONDS: f64 = 60.0;
/// Hard ceiling for one suppressed round, matching the sample TTL horizon.
pub const MAX_DELAY_SECONDS: f64 = 900.0;
/// Failure count at which the cap is reached; further failures stay there.
const MAX_FAILURES: u32 = 5;

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
                .min(MAX_DELAY_SECONDS / BASE_DELAY_SECONDS);
        let until = at + delay.min(MAX_DELAY_SECONDS);
        self.entries.insert(key, (failures, until));
        until
    }

    /// Suppresses until an explicit bounded horizon, as an endpoint's
    /// `retry-after` declared; values beyond the cap clamp to it.
    pub fn suppress_until(&mut self, account: &AccountId, source: &str, at: f64, until: f64) {
        let until = until.clamp(at, at + MAX_DELAY_SECONDS);
        self.entries
            .entry((account.as_str().to_owned(), source.to_owned()))
            .and_modify(|entry| entry.1 = entry.1.max(until))
            .or_insert((1, until));
    }

    /// Clears any suppression after one successful round.
    pub fn record_success(&mut self, account: &AccountId, source: &str) {
        self.entries
            .remove(&(account.as_str().to_owned(), source.to_owned()));
    }

    /// Loads the durable backoff ledger under `home`; a missing or malformed
    /// ledger starts clean rather than blocking collection.
    pub fn load(home: &Path) -> Self {
        let Ok(text) = std::fs::read_to_string(home.join("capacity/backoff.json")) else {
            return Self::default();
        };
        serde_json::from_str::<Vec<((String, String), (u32, f64))>>(&text)
            .map(|rows| Self {
                entries: rows.into_iter().collect(),
            })
            .unwrap_or_default()
    }

    /// Persists the ledger with expired entries pruned; write failures are
    /// returned so a read-only home is reported, not silently ignored.
    pub fn save(&self, home: &Path, at: f64) -> std::io::Result<()> {
        let dir = home.join("capacity");
        std::fs::create_dir_all(&dir)?;
        let rows: Vec<_> = self
            .entries
            .iter()
            .filter(|(_, (_, until))| *until > at)
            .collect();
        let text = serde_json::to_string(&rows).unwrap_or_else(|_| "[]".into());
        std::fs::write(dir.join("backoff.json"), text)
    }
}

/// One account/executable observation, shared across every provider alias.
struct Planned {
    /// Stable physical account identity supplied by the registry.
    account: AccountId,
    /// First provider alias, used only for the observation's display scope.
    provider: ProviderId,
    /// Validated executable configuration shared by all aliases of this source.
    binding: CollectorBinding,
    /// Source key which survives normal script edits and never comes from stdout.
    source: String,
    /// Union of configured model definitions, keyed by their native lane.
    models: BTreeMap<String, Value>,
}

/// Returns a stable configured identity, or a digest of command and literal arguments.
fn source(binding: &CollectorBinding) -> String {
    if !binding.source.is_empty() {
        return binding.source.clone();
    }
    format!(
        "exec:{}",
        agent_run_domain::canonical::sha256_hex(
            &json!({"command": binding.command, "args": binding.args}),
            true
        )
    )
}

/// Resolves one execution per account/source, rejecting conflicting alias settings.
/// No credentials, network, subprocesses or database writes occur here.
fn plan(catalog: &ProviderCatalog) -> Result<Vec<Planned>> {
    let mut units: BTreeMap<(AccountId, String), Planned> = BTreeMap::new();
    for provider in catalog.providers() {
        if provider.limits_source == LimitsSource::None {
            continue;
        }
        if provider.limits_source != LimitsSource::Exec {
            return Err(crate::error::invalid(
                "built-in quota collectors are retired; configure exec",
            ));
        }
        let binding = provider
            .collector
            .as_ref()
            .ok_or_else(|| crate::error::invalid("collector missing"))?;
        binding.validate()?;
        let source = source(binding);
        for bound in &provider.bindings {
            let key = (bound.account.clone(), source.clone());
            let unit = units.entry(key).or_insert_with(|| Planned {
                account: bound.account.clone(),
                provider: provider.id.clone(),
                binding: binding.clone(),
                source: source.clone(),
                models: BTreeMap::new(),
            });
            if unit.binding != *binding {
                return Err(crate::error::invalid(
                    "aliases of one account declared contradictory collector bindings",
                ));
            }
            if catalog
                .account(&bound.account)
                .is_none_or(|a| a.status != AccountStatus::Enabled)
            {
                continue;
            }
            for model in &provider.models {
                if bound
                    .models
                    .as_ref()
                    .is_some_and(|ids| !ids.contains(&model.id))
                {
                    continue;
                }
                let lane = model.native_model.as_ref().unwrap_or(&model.id).clone();
                unit.models
                    .insert(lane.clone(), json!({"id":model.id, "native_model":lane}));
            }
        }
    }
    Ok(units.into_values().collect())
}

/// Produces private invocation context for the selected account, never for a provider alias.
/// OAuth/API tokens travel only over stdin; native Codex retains its harness-owned login.
/// No provider endpoint or quota response format is interpreted in this function.
fn context(
    home: &Path,
    config: &ProviderConfig,
    catalog: &ProviderCatalog,
    unit: &Planned,
    at: f64,
) -> Result<Value> {
    let account = catalog
        .account(&unit.account)
        .ok_or_else(|| crate::error::invalid(CREDENTIAL_UNAVAILABLE))?;
    let provider = catalog
        .provider(&unit.provider)
        .ok_or_else(|| crate::error::invalid("provider missing"))?;
    let harness = config
        .harnesses
        .get(&provider.harness)
        .ok_or_else(|| crate::error::invalid("harness missing"))?;
    let reference = CredentialRef::from_secret(&account.secret_ref)?;
    let auth = match &reference {
        CredentialRef::Native(HarnessId::Codex) => {
            let directory = std::env::var_os("CODEX_HOME")
                .filter(|v| !v.is_empty())
                .map(PathBuf::from)
                .or_else(|| std::env::var_os("HOME").map(|v| PathBuf::from(v).join(".codex")))
                .ok_or_else(|| crate::error::invalid(CREDENTIAL_UNAVAILABLE))?;
            json!({"kind":"native_login", "directory":directory})
        }
        CredentialRef::Named {
            harness: HarnessId::Codex,
            label,
        } => {
            json!({"kind":"native_login", "directory":home.join("accounts/codex").join(label.as_str())})
        }
        _ => {
            let reader = QuotaCredentialReader::from_host(home.to_owned(), harness.home.clone())
                .ok_or_else(|| crate::error::invalid(CREDENTIAL_UNAVAILABLE))?;
            let token = reader
                .read(&reference)
                .map_err(|_| crate::error::invalid(CREDENTIAL_UNAVAILABLE))?;
            json!({"kind":"token", "token":token})
        }
    };
    Ok(
        json!({"version":1, "account":{"id":account.account_id,"auth_family":account.auth_family},
        "models":unit.models, "now":at, "auth":auth,
        "harness":{"id":provider.harness,"command":harness.binary}}),
    )
}

/// Rejects a credential in any output string or key before facts reach durable state.
fn contains_secret(value: &Value, secret: &str) -> bool {
    match value {
        Value::String(text) => text.contains(secret),
        Value::Array(items) => items.iter().any(|item| contains_secret(item, secret)),
        Value::Object(fields) => fields
            .iter()
            .any(|(key, value)| key.contains(secret) || contains_secret(value, secret)),
        _ => false,
    }
}

/// Executes account-scoped external collectors and persists only validated successful facts.
/// Failed, disabled or backoff-suppressed accounts keep their previous samples and latches.
/// All subprocess and credential work finishes before opening a persistence transaction.
pub async fn collect_providers(home: &Path, config: &ProviderConfig) -> Result<Value> {
    let accounts = agent_run_store::Store::open(home)?.list_accounts()?;
    let catalog = config.resolve_catalog(accounts)?;
    let units = plan(&catalog)?;
    let mut backoff = AccountBackoff::load(home);
    let mut results = Vec::new();
    let mut all_ok = true;
    for unit in units {
        let at = now();
        let mut issues = Vec::new();
        let mut windows = 0;
        if catalog
            .account(&unit.account)
            .is_none_or(|a| a.status != AccountStatus::Enabled)
        {
            issues.push("account_disabled");
        } else if unit.models.is_empty() {
            issues.push("no_bound_models");
        } else if backoff.suppressed(&unit.account, &unit.source, at) {
            issues.push("backoff");
        } else {
            let outcome = async {
                let context = context(home, config, &catalog, &unit, at)
                    .map_err(|_| CREDENTIAL_UNAVAILABLE)?;
                let raw = executable::run(&unit.binding, &context, home).await?;
                if context["auth"]["token"]
                    .as_str()
                    .is_some_and(|token| !token.is_empty() && contains_secret(&raw, token))
                {
                    return Err("collector_output_contains_credential");
                }
                let scope = CollectorScope {
                    runtime: unit.provider.as_str().into(),
                    source: unit.source.clone(),
                    models: unit.models.keys().cloned().collect(),
                };
                let snapshot = normalize_collector_output(
                    &unit.account,
                    &scope,
                    &raw,
                    now(),
                    MAX_OUTPUT_WINDOWS,
                    MAX_OUTPUT_MODELS,
                )
                .map_err(|_| "collector_output_invalid")?;
                let count = snapshot
                    .models
                    .iter()
                    .flat_map(|m| &m.pools)
                    .flat_map(|p| p.windows.iter().map(|w| (p.key.as_str(), w.name.as_str())))
                    .collect::<BTreeSet<_>>()
                    .len();
                agent_run_store::quota::record_quota_snapshot(
                    home,
                    unit.provider.as_str(),
                    &snapshot,
                    config.capacity.sample_retention,
                    now(),
                )
                .map_err(|_| STORE_FAILED)?;
                Ok::<_, &'static str>(count)
            }
            .await;
            match outcome {
                Ok(count) => {
                    windows = count;
                    backoff.record_success(&unit.account, &unit.source);
                }
                Err(code) => {
                    issues.push(code);
                    backoff.record_failure(&unit.account, &unit.source, at);
                }
            }
        }
        let status = if issues.is_empty() {
            if windows == 0 {
                "no_data"
            } else {
                "collected"
            }
        } else {
            "failed"
        };
        all_ok &= status != "failed";
        results.push(
            json!({"account":unit.account,"source":unit.source,"runtime":unit.provider,
            "status":status,"windows":windows,"issues":issues}),
        );
    }
    let mut report = json!({"ok":all_ok,"results":results});
    if backoff.save(home, now()).is_err() {
        report["ok"] = json!(false);
        report["backoff_persist"] = json!(BACKOFF_PERSIST_FAILED);
    }
    Ok(report)
}
