//! Account-scoped first-party quota collection over registered accounts.
//!
//! One global [`AccountId`] owns one physical quota pool per lane across every
//! provider alias, so this driver collects once per `(account, collector)`
//! pair per round regardless of how many labels or providers bind it.
//! Credentials are resolved in Rust only: explicit protected stores
//! (environment, private file, Keychain) through the shared
//! [`CredentialReader`], plus a narrow quota-only bridge that reads the
//! Claude harness's own OAuth store for native/named Claude logins — tokens
//! are never exported to Lua, configuration, diagnostics, or the store, and
//! the generic custom-gateway reader's native refusal is left untouched.
//! All network work happens outside every database transaction; persistence
//! goes through [`agent_run_store::quota::record_quota_snapshot`] only after
//! a collector round has fully succeeded.

use crate::{
    capacity::lua::{
        run_collector, AllowedOrigin, AuthCapability, AuthPlacement, CollectorError,
        CollectorLimits, CollectorScript, QuotaHttpClient, ScriptRegistry,
    },
    capacity::quota::{normalize_collector_output, CollectorScope},
    domain::now,
};
use agent_run_adapters::authorized_request::{CredentialReader, SystemCredentialReader};
use agent_run_config::provider_config::ProviderConfig;
use agent_run_domain::{
    catalog::{
        AccountId, AccountRecord, AccountStatus, CollectorBinding, CredentialPlacement,
        LimitsSource, ProviderCatalog, ProviderId,
    },
    CredentialRef, Error, Result,
};
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
    str::FromStr,
    sync::{Arc, Mutex, OnceLock},
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
pub struct AccountBackoff {
    entries: BTreeMap<(String, String), (u32, f64)>,
}

/// First failed-round delay; later rounds double it up to the cap.
const BASE_DELAY_SECONDS: f64 = 60.0;
/// Hard ceiling for one suppressed round, matching the sample TTL horizon.
pub const MAX_DELAY_SECONDS: f64 = 900.0;
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

/// Process-wide retained-script registry for configured custom collectors.
///
/// A custom script revision is installed only when it verifies and compiles;
/// a rejected replacement never displaces the retained valid revision, so a
/// bad file edit cannot break a working polling loop.
fn custom_registry() -> &'static Mutex<ScriptRegistry> {
    static REGISTRY: OnceLock<Mutex<ScriptRegistry>> = OnceLock::new();
    REGISTRY.get_or_init(|| Mutex::new(ScriptRegistry::default()))
}

/// Quota-side protected resolver including the narrow native-login bridge.
///
/// Explicit env/file/Keychain references delegate to the shared system
/// reader unchanged. Native and named Claude logins resolve through the
/// Claude harness's own store (credentials file, else the service-keyed
/// Keychain item) and only inside Rust; native Codex logins are refused
/// because Codex quota travels through app-server metadata, not an
/// exportable token. This does not alter the custom-gateway reader, which
/// keeps refusing native logins for gateway headers.
pub struct QuotaCredentialReader {
    claude_home: PathBuf,
}

impl QuotaCredentialReader {
    /// Builds the reader for the configured Claude harness home.
    pub fn new(claude_home: PathBuf) -> Self {
        Self { claude_home }
    }

    /// Reads the Claude harness's own OAuth token without exporting it.
    fn claude_oauth(&self) -> Result<String> {
        let file = self.claude_home.join(".credentials.json");
        if file.is_file() {
            let secret = SystemCredentialReader
                .read(&CredentialRef::File(file))
                .ok()
                .filter(|text| !text.is_empty());
            if let Some(text) = secret {
                if let Ok(value) = serde_json::from_str::<Value>(&text) {
                    let token = value
                        .pointer("/claudeAiOauth/accessToken")
                        .or_else(|| value.get("accessToken"))
                        .and_then(Value::as_str)
                        .filter(|token| !token.is_empty());
                    if let Some(token) = token {
                        return Ok(token.to_owned());
                    }
                }
            }
        }
        // Claude's legacy Keychain item is keyed by service only.
        agent_run_platform::keychain::generic_password_service("Claude Code-credentials")
            .and_then(|text| {
                serde_json::from_str::<Value>(&text).ok().and_then(|value| {
                    value
                        .pointer("/claudeAiOauth/accessToken")
                        .or_else(|| value.get("accessToken"))
                        .and_then(Value::as_str)
                        .map(str::to_owned)
                })
            })
            .filter(|token| !token.is_empty())
            .ok_or_else(|| crate::error::invalid("claude native quota token is unavailable"))
    }
}

impl CredentialReader for QuotaCredentialReader {
    fn read(&self, reference: &CredentialRef) -> Result<String> {
        match reference {
            CredentialRef::Native(agent_run_domain::catalog::HarnessId::ClaudeCode)
            | CredentialRef::Named {
                harness: agent_run_domain::catalog::HarnessId::ClaudeCode,
                ..
            } => self.claude_oauth(),
            CredentialRef::Native(agent_run_domain::catalog::HarnessId::Codex)
            | CredentialRef::Named {
                harness: agent_run_domain::catalog::HarnessId::Codex,
                ..
            } => Err(crate::error::invalid(
                "codex quota uses app-server metadata; no exportable token",
            )),
            _ => SystemCredentialReader.read(reference),
        }
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
/// the `anthropic-beta: oauth-2025-04-20` header the endpoint requires.
/// Only kinds with a recorded contract are reported: `session` maps to the
/// `primary` lane's `five_hour` window and `weekly_all` to
/// `secondary`/`seven_day`. Model-scoped weekly kinds and every unknown
/// kind are **ignored** — mapping them to an all-model pool would invent
/// governing quota — and a payload with no known kind fails typed instead
/// of producing fabricated windows. The live envelope remains unverified;
/// any drift surfaces as the engine's typed categories, never invented
/// facts.
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
    local lane = nil
    local window = nil
    if entry.kind == "session" then
      lane = "primary"
      window = "five_hour"
    elseif entry.kind == "weekly_all" then
      lane = "secondary"
      window = "seven_day"
    end
    if lane ~= nil and entry.percent ~= nil then
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
  end
  if #windows == 0 then
    error("usage payload has no known usage kind")
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
    /// The first-party collector this unit runs; `None` marks a configured
    /// custom script identity (or an unknown one, reported typed).
    collector: Option<&'static FirstPartyCollector>,
    /// The configured script identity spelling, for reports and registries.
    script: String,
    /// The one canonical binding every alias of this unit agreed on.
    binding: CollectorBinding,
    /// Union of the eligible bindings' effective model subsets, keyed by each
    /// model's `native_model.unwrap_or(id)` spelling.
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
/// union of each eligible binding's effective model subset (the binding's own
/// subset, or the provider's full set when it inherits), keyed by native
/// spelling — so one physical account yields one remote request set and one
/// pool family. Every alias of one `(account, script)` pair must declare the
/// exact same binding; a contradiction is rejected here, before any
/// credential access or network request, instead of being resolved by
/// declaration order. An unknown script identity stays in the plan so its
/// round reports the typed `collector_unknown` failure rather than being
/// silently dropped.
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
        let eligible = |bound: &agent_run_domain::catalog::ProviderBinding| -> bool {
            catalog
                .account(&bound.account)
                .is_some_and(|record| record.status == AccountStatus::Enabled)
        };
        for bound in &provider.bindings {
            let key = (bound.account.as_str().to_owned(), binding.script.clone());
            match units.entry(key) {
                std::collections::btree_map::Entry::Vacant(slot) => {
                    slot.insert(Planned {
                        collector: first_party(&binding.script),
                        script: binding.script.clone(),
                        binding: binding.clone(),
                        models: BTreeMap::new(),
                        runtime: provider.id.clone(),
                        account: bound.account.clone(),
                    });
                }
                std::collections::btree_map::Entry::Occupied(slot) => {
                    if slot.get().binding != *binding {
                        return Err(Error::Validation(
                            "aliases of one account declared contradictory collector bindings"
                                .into(),
                        ));
                    }
                }
            }
            if !eligible(bound) {
                continue;
            }
            let unit = &mut units
                .get_mut(&(bound.account.as_str().to_owned(), binding.script.clone()))
                .expect("unit was just inserted");
            let provider_models = &provider.models;
            for model in bound
                .models
                .as_ref()
                .map(|ids| {
                    ids.iter()
                        .map(|id| {
                            provider_models
                                .iter()
                                .find(|model| &model.id == id)
                                .expect("validated subset names a provider model")
                        })
                        .collect::<Vec<_>>()
                })
                .unwrap_or_else(|| provider_models.iter().collect())
            {
                let native = model
                    .native_model
                    .clone()
                    .unwrap_or_else(|| model.id.clone());
                let value = json!({"id": model.id, "native_model": native});
                unit.models.insert(native, value);
            }
        }
    }
    Ok(units.into_values().collect())
}

/// Resolves the runnable script and placement for one planned unit.
///
/// First-party collectors contribute their frozen bytes and placement. A
/// custom identity runs the retained registry revision of its configured
/// script file: the file's current bytes are installed only when they verify
/// and compile, so a bad replacement never displaces the last valid
/// revision, and a first-party identity with a script file is a typed
/// conflict rather than a silent override.
fn resolve_script(unit: &Planned) -> std::result::Result<(String, AuthPlacement), CollectorError> {
    if let Some(collector) = unit.collector {
        if unit.binding.script_file.is_some() || unit.binding.auth.is_some() {
            return Err(CollectorError::InvalidOutput("first_party_override"));
        }
        return Ok((collector.script.to_owned(), collector.placement.clone()));
    }
    let Some(file) = unit.binding.script_file.clone() else {
        return Err(CollectorError::InvalidOutput("collector_unknown"));
    };
    let placement = match unit.binding.auth {
        Some(CredentialPlacement::RawAuthorization) => AuthPlacement::RawAuthorization,
        Some(CredentialPlacement::BearerAuthorization) => AuthPlacement::BearerAuthorization,
        None => return Err(CollectorError::InvalidOutput("collector_auth_unbound")),
    };
    let limits = CollectorLimits::default();
    let mut registry = custom_registry()
        .lock()
        .map_err(|_| CollectorError::Internal("registry"))?;
    let mut installed = false;
    if let Ok(text) = std::fs::read_to_string(&file) {
        let script = CollectorScript::new(text);
        if registry.install(&unit.script, script, &limits).is_ok() {
            installed = true;
        }
    }
    if !installed && registry.get(&unit.script).is_none() {
        return Err(CollectorError::InvalidOutput(
            "collector_script_unavailable",
        ));
    }
    let script = registry
        .get(&unit.script)
        .expect("retained or freshly installed")
        .source
        .clone();
    Ok((script, placement))
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

/// Runs one account-scoped quota collection round for a whole catalog.
///
/// For every deduplicated `(account, collector)` unit whose account is
/// registered and enabled and not currently suppressed by `backoff`, resolves
/// the protected credential, runs the resolved script in the bounded Lua
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
        // The stable source identity never changes with a script revision;
        // custom scripts use their configured identity verbatim.
        let source = unit
            .collector
            .map(|collector| collector.source)
            .unwrap_or(unit.script.as_str());
        let entry = json!({
            "account": unit.account.as_str(),
            "source": source,
            "runtime": unit.runtime.as_str(),
        });
        let record = catalog.account(&unit.account);
        let mut issues = Vec::new();
        let mut windows = 0usize;
        let at = now();
        if record.is_none_or(|record| record.status != AccountStatus::Enabled) {
            issues.push("account_disabled".to_owned());
        } else if backoff.suppressed(&unit.account, source, at) {
            issues.push("backoff".to_owned());
        } else {
            let record: &AccountRecord = record.expect("enabled record");
            match resolve_script(&unit) {
                Err(error) => issues.push(error.to_string()),
                Ok((text, placement)) => {
                    match capability(&unit.account, record, &unit.binding, &placement, reader) {
                        Ok(auth) => {
                            let origin = unit
                                .binding
                                .origins
                                .first()
                                .expect("validated binding has an origin");
                            let script = CollectorScript::new(text);
                            let scope = CollectorScope {
                                runtime: unit.runtime.as_str().to_owned(),
                                source: source.to_owned(),
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
                                    // All network work is complete before the
                                    // store opens its persistence transaction.
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
                                            backoff.record_success(&unit.account, source);
                                        }
                                        Err(error) => issues.push(static_reason(&error)),
                                    }
                                }
                                Err(CollectorError::RateLimited(horizon)) => {
                                    // An endpoint-declared horizon overrides the
                                    // exponential default, bounded by the cap.
                                    backoff.record_failure(&unit.account, source, at);
                                    if let Some(seconds) = horizon {
                                        backoff.suppress_until(
                                            &unit.account,
                                            source,
                                            at,
                                            at + seconds as f64,
                                        );
                                    }
                                    issues.push("quota_collector_rate_limited".to_owned());
                                }
                                Err(error) => {
                                    backoff.record_failure(&unit.account, source, at);
                                    issues.push(error.to_string());
                                }
                            }
                        }
                        Err(error) => {
                            backoff.record_failure(&unit.account, source, at);
                            issues.push(static_reason(&error));
                        }
                    }
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

/// Reduces one domain error to a bounded static reason.
///
/// Only the reader's own fixed code families pass through verbatim; any other
/// text — including a malicious or echoing credential store's error string —
/// collapses into the generic `credential_unavailable` bucket so no
/// credential material can reach a round report.
fn static_reason(error: &Error) -> String {
    let text = match error {
        Error::Validation(reason) | Error::Runtime(reason) => reason,
        _ => return "source_failed".to_owned(),
    };
    const SAFE_PREFIXES: &[&str] = &[
        "credential",
        "native login",
        "codex quota uses",
        "claude native quota",
        "invalid ",
        "unsupported ",
        "glm requires",
    ];
    if text.len() <= 128 && SAFE_PREFIXES.iter().any(|prefix| text.starts_with(prefix)) {
        return text.clone();
    }
    "credential_unavailable".to_owned()
}

/// Stable collector source identity for Codex app-server observations.
const CODEX_SOURCE: &str = "codex-appserver";

/// Maps one app-server `account/rateLimits/read` response into the validated
/// version-1 collector shape and normalizes it for `account`.
///
/// Buckets become physical lanes keyed by their limit id; each present
/// primary/secondary window contributes one entry over the provider's whole
/// model set, and `resetsAt` stays numeric epoch seconds. Reusing
/// [`normalize_collector_output`] means the same closed validation guards
/// app-server facts as Lua facts. Unknown or malformed fields fail typed
/// instead of being dropped silently, mirroring `normalize_codex`.
fn codex_snapshot(
    account: &AccountId,
    runtime: &str,
    models: &BTreeSet<String>,
    response: &Value,
    observed: f64,
) -> Result<agent_run_domain::catalog::NormalizedQuotaSnapshot> {
    let raw = response
        .get("result")
        .filter(|value| value.is_object())
        .unwrap_or(response);
    let buckets = raw
        .get("rateLimitsByLimitId")
        .and_then(Value::as_object)
        .ok_or_else(|| crate::error::invalid("codex quota response has no bucket map"))?;
    let mut windows = Vec::new();
    for (limit_id, bucket) in buckets {
        if limit_id.is_empty() || !bucket.is_object() {
            continue;
        }
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
            let reset_at = value
                .get("resetsAt")
                .and_then(Value::as_f64)
                .filter(|reset| reset.is_finite() && *reset >= 0.0);
            windows.push(json!({
                "pool": limit_id,
                "window": crate::capacity::sources::window_name(minutes),
                "models": models,
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
    normalize_collector_output(
        account,
        &scope,
        &json!({"version": 1, "windows": windows}),
        observed,
        256,
        256,
    )
}

/// One app-server round for every CodexAppserver provider in a v2 catalog.
///
/// Each enabled binding's label selects its isolated probe home exactly as
/// the v1 path does; the verified process cleanup contract of
/// [`crate::capacity::sources::codex_probe`] is reused unchanged. Probing is
/// network work and completes before any store transaction opens.
async fn codex_appserver_round(
    home: &Path,
    config: &ProviderConfig,
    catalog: &ProviderCatalog,
    backoff: &mut AccountBackoff,
    retention: usize,
) -> Result<Vec<Value>> {
    let mut results = Vec::new();
    let claude = config
        .harnesses
        .get(&agent_run_domain::catalog::HarnessId::Codex)
        .expect("validated config declares the codex harness");
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
    for provider in catalog.providers() {
        if provider.limits_source != LimitsSource::CodexAppserver {
            continue;
        }
        let models: BTreeSet<String> = provider
            .models
            .iter()
            .map(|model| {
                model
                    .native_model
                    .clone()
                    .unwrap_or_else(|| model.id.clone())
            })
            .collect();
        for bound in &provider.bindings {
            let at = now();
            let mut issues = Vec::new();
            let mut windows = 0usize;
            let record = catalog.account(&bound.account);
            if record.is_none_or(|record| record.status != AccountStatus::Enabled) {
                issues.push("account_disabled".to_owned());
            } else if backoff.suppressed(&bound.account, CODEX_SOURCE, at) {
                issues.push("backoff".to_owned());
            } else {
                let runtime = serde_json::from_value::<crate::config::Runtime>(json!({
                    "enabled": true,
                    "adapter": "codex",
                    "binary": claude.binary,
                    "home": claude.home,
                    "models": models,
                }))
                .map_err(|_| crate::error::invalid("invalid synthesized codex runtime"))?;
                let observed = now();
                let probe = crate::capacity::sources::codex_probe(
                    home,
                    &legacy,
                    &runtime,
                    Some(bound.label.as_str()),
                    "account/rateLimits/read",
                )
                .await;
                let outcome = probe.and_then(|value| {
                    codex_snapshot(
                        &bound.account,
                        provider.id.as_str(),
                        &models,
                        &value,
                        observed,
                    )
                });
                match outcome {
                    Ok(snapshot) => {
                        match agent_run_store::quota::record_quota_snapshot(
                            home,
                            provider.id.as_str(),
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
                                backoff.record_success(&bound.account, CODEX_SOURCE);
                            }
                            Err(error) => issues.push(static_reason(&error)),
                        }
                    }
                    Err(_) => {
                        backoff.record_failure(&bound.account, CODEX_SOURCE, at);
                        issues.push("probe_failed".to_owned());
                    }
                }
            }
            let status = if issues.is_empty() && windows > 0 {
                "collected"
            } else if issues.is_empty() {
                "no_data"
            } else {
                "failed"
            };
            results.push(finish(
                json!({
                    "account": bound.account.as_str(),
                    "source": CODEX_SOURCE,
                    "runtime": provider.id.as_str(),
                }),
                status,
                windows,
                issues,
            ));
        }
    }
    Ok(results)
}

/// One full account-scoped polling round for a v2 provider configuration.
///
/// Resolves the catalog against the registered account store, runs the Lua
/// collector units and the Codex app-server units, and persists the durable
/// backoff ledger so suppression survives the process boundary between
/// polling rounds. Report rows carry only static typed facts.
pub async fn collect_providers(home: &Path, config: &ProviderConfig) -> Result<Value> {
    let store = agent_run_store::Store::open(home)
        .map_err(|_| Error::Runtime("account registry is unavailable".into()))?;
    let catalog = config.resolve_catalog(store.list_accounts()?)?;
    let limits = CollectorLimits::default();
    let client = Arc::new(
        crate::capacity::lua::ReqwestQuotaHttp::new(limits.http_response_body_bytes)
            .map_err(|_| Error::Runtime("quota transport unavailable".into()))?,
    );
    let reader = QuotaCredentialReader::new(
        config
            .harnesses
            .get(&agent_run_domain::catalog::HarnessId::ClaudeCode)
            .map(|harness| harness.home.clone())
            .unwrap_or_else(|| PathBuf::from("/nonexistent")),
    );
    let retention = config.capacity.sample_retention;
    let mut backoff = AccountBackoff::load(home);
    let mut report = collect_provider_quota(
        home,
        &catalog,
        retention,
        &limits,
        client,
        &reader,
        &mut backoff,
    )
    .await?;
    let codex = codex_appserver_round(home, config, &catalog, &mut backoff, retention).await?;
    let at = now();
    if let Err(error) = backoff.save(home, at) {
        report["backoff_persist"] = json!(format!("runtime:{error}"));
    }
    let ok = report["ok"].as_bool().unwrap_or(false);
    if let Some(rows) = report["results"].as_array_mut() {
        rows.extend(codex);
    }
    report["ok"] = json!(ok);
    Ok(report)
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
