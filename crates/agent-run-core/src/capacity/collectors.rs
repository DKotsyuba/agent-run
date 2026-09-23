//! Account-scoped first-party quota collection over registered accounts.
//!
//! One global [`AccountId`] owns one physical quota pool per lane across every
//! provider alias, so this driver collects once per `(account, collector)`
//! pair per round regardless of how many labels or providers bind it.
//! Credentials are resolved in Rust only through
//! [`QuotaCredentialReader`] (see [`super::quota_auth`]); tokens are never
//! exported to Lua, configuration, diagnostics, or the store, and every
//! resolution failure is the one fixed `credential_unavailable` code.
//! Codex accounts are observed through the app-server probe in
//! [`super::codex_quota`]. All network work happens outside every database
//! transaction; persistence goes through
//! [`agent_run_store::quota::record_quota_snapshot`] only after a collector
//! round has fully succeeded.

pub use super::quota_auth::QuotaCredentialReader;
use super::quota_auth::{BACKOFF_PERSIST_FAILED, CREDENTIAL_UNAVAILABLE, STORE_FAILED};
use crate::{
    capacity::lua::{
        run_collector, AllowedOrigin, AuthCapability, AuthPlacement, CollectorError,
        CollectorLimits, CollectorScript, QuotaHttpClient, ScriptRegistry,
    },
    capacity::quota::CollectorScope,
    domain::now,
};
use agent_run_adapters::authorized_request::CredentialReader;
use agent_run_config::provider_config::ProviderConfig;
use agent_run_domain::{
    catalog::{
        AccountId, AccountRecord, AccountStatus, CollectorBinding, CredentialPlacement,
        LimitsSource, ProviderCatalog, ProviderId,
    },
    CredentialRef, Error, Result,
};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    io::Read,
    path::Path,
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

/// Process-wide retained-script registry for configured custom collectors.
///
/// Entries are keyed by the complete source identity (script id plus the
/// canonical script file), so two configurations reusing one custom id never
/// share code. A revision is installed only when it verifies and compiles; a
/// rejected replacement never displaces the retained valid revision. The
/// durable copy under `capacity/scripts/` carries that guarantee across the
/// process boundary between polling rounds.
fn custom_registry() -> &'static Mutex<ScriptRegistry> {
    static REGISTRY: OnceLock<Mutex<ScriptRegistry>> = OnceLock::new();
    REGISTRY.get_or_init(|| Mutex::new(ScriptRegistry::default()))
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

/// First-party GLM Coding Plan usage collector.
///
/// `GET {origin}/api/monitor/usage/quota/limit` with the raw `Authorization`
/// token the engine injects (the quota endpoint is not the inference
/// gateway's Bearer style). The payload root is `data` when present, else the
/// top-level object. Two `limits[].type` families govern inference:
///
/// - current `CREDIT_LIMIT` pools, one entry per window, whose `unit`/`number`
///   encoding is `3`/`5` for five hours and `6`/`1` for one week (observed on
///   the live account and in the reporting author's payload), with
///   `nextResetTime` in epoch milliseconds, `usage` as the total and
///   `currentValue` as the used amount;
/// - the older official `TOKENS_LIMIT`, whose bare `percentage` is the
///   five-hour usage (the same `unit`/`number` encoding applies when present).
///
/// Every window lands in the `primary` pool under a distinct `five_hour` or
/// `seven_day` identity. `percentage` must agree with the absolute counts
/// within one point when both are present (the provider rounds it); counts
/// alone derive it. `remaining` is not used: live values do not equal
/// `usage - currentValue`. `TIME_LIMIT` is monthly MCP usage and is never
/// converted into inference capacity. Any other type, window encoding,
/// out-of-range number, or reset outside year 1..9999 fails the round typed
/// rather than inventing or silently omitting a pool.
const GLM_QUOTA: FirstPartyCollector = FirstPartyCollector {
    id: "glm_quota",
    source: "glm-quota",
    placement: AuthPlacement::RawAuthorization,
    script: r#"
collect = function(ctx)
  local null = ctx.json.null
  local function present(value)
    return value ~= nil and value ~= null
  end
  local function number(value)
    return type(value) == "number" and value == value
  end
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
  if type(payload) ~= "table" then
    error("quota payload malformed")
  end
  local data = payload.data
  if not present(data) then
    data = payload
  end
  if type(data) ~= "table" or type(data.limits) ~= "table" then
    error("quota payload has no limits")
  end
  local windows = {}
  for _, item in ipairs(data.limits) do
    if type(item) ~= "table" then
      error("quota limit malformed")
    end
    local kind = item.type
    if kind == "TOKENS_LIMIT" or kind == "CREDIT_LIMIT" then
      local window = nil
      if kind == "TOKENS_LIMIT" and not present(item.unit) and not present(item.number) then
        window = "five_hour"
      elseif item.unit == 3 and item.number == 5 then
        window = "five_hour"
      elseif item.unit == 6 and item.number == 1 then
        window = "seven_day"
      else
        error("quota limit window unknown")
      end
      local used = nil
      if present(item.percentage) then
        if not number(item.percentage) or item.percentage < 0 or item.percentage > 100 then
          error("quota percentage invalid")
        end
        used = item.percentage
      end
      if present(item.usage) or present(item.currentValue) then
        local total = item.usage
        local current = item.currentValue
        if not number(total) or not number(current) or total <= 0
          or current < 0 or current > total then
          error("quota counts invalid")
        end
        local counted = current * 100.0 / total
        if used == nil then
          used = counted
        elseif used - counted > 1.0 or counted - used > 1.0 then
          error("quota counts disagree")
        end
      end
      if used == nil then
        error("quota usage absent")
      end
      local reset_at = nil
      if present(item.nextResetTime) then
        local ms = item.nextResetTime
        if not number(ms) or ms <= 0 or ms > 253402300799000 then
          error("quota reset invalid")
        end
        reset_at = ms / 1000.0
      end
      windows[#windows + 1] = {
        pool = "primary",
        window = window,
        models = models,
        remaining_percent = 100.0 - used,
        reset_at = reset_at,
        observed_at = ctx.now
      }
    elseif kind ~= "TIME_LIMIT" then
      error("quota limit type unknown")
    end
  end
  if #windows == 0 then
    error("quota payload has no inference limit")
  end
  return { version = 1, windows = windows }
end
"#,
};

/// First-party Anthropic OAuth usage collector.
///
/// `GET {origin}/api/oauth/usage` with the engine-injected Bearer token and
/// the `anthropic-beta: oauth-2025-04-20` header the endpoint requires.
/// Only the `limits[]` envelope is read — the parallel top-level
/// `five_hour`/`seven_day*` objects describe the same pools and are never
/// unioned in. Each entry needs a numeric `percent` in `0..=100` and, when
/// present, an RFC 3339 `resets_at`:
///
/// - `session` → `primary`/`five_hour` over every bound model;
/// - `weekly_all` → `secondary`/`seven_day` over every bound model;
/// - `weekly_scoped` → its own `model:<key>`/`seven_day` pool over exactly
///   the bound models its `scope.model` names: an exact native `id` match, or
///   a model whose name tokens contain every `display_name` token (live
///   payloads carry `{"display_name": "Fable", "id": null}`). A scoped limit
///   naming no bound model is not applicable to this account and is skipped.
///
/// `is_active` only marks the currently binding limit (observed `true` on the
/// critical scoped entry, `false` on the others); every entry constrains its
/// models regardless, so it never filters. An unknown `kind`, a non-null
/// `scope.surface`, an unusable model scope, or a scope on a general kind
/// fails the round typed instead of inventing or omitting a governing pool.
const ANTHROPIC_USAGE: FirstPartyCollector = FirstPartyCollector {
    id: "anthropic_usage",
    source: "anthropic-usage",
    placement: AuthPlacement::BearerAuthorization,
    script: r#"
collect = function(ctx)
  local null = ctx.json.null
  local function present(value)
    return value ~= nil and value ~= null
  end
  local models = {}
  local model_tokens = {}
  for name, _ in pairs(ctx.models) do
    models[#models + 1] = name
    local set = {}
    for _, token in ipairs(ctx.text.tokens(name) or {}) do
      set[token] = true
    end
    model_tokens[name] = set
  end
  local function scoped(scope)
    if type(scope) ~= "table" or present(scope.surface) or type(scope.model) ~= "table" then
      error("usage scope unknown")
    end
    local id = scope.model.id
    local display = scope.model.display_name
    local wanted = nil
    if type(display) == "string" then
      wanted = ctx.text.tokens(display)
    elseif present(display) then
      error("usage scope unknown")
    end
    if present(id) and type(id) ~= "string" then
      error("usage scope unknown")
    end
    local key = nil
    if type(id) == "string" and id ~= "" then
      key = id
    elseif wanted ~= nil and #wanted > 0 then
      key = wanted[1]
      for index = 2, #wanted do
        key = key .. "-" .. wanted[index]
      end
    else
      error("usage scope unknown")
    end
    local found = {}
    for _, name in ipairs(models) do
      local member = name == id
      if not member and wanted ~= nil and #wanted > 0 then
        member = true
        for _, token in ipairs(wanted) do
          if not model_tokens[name][token] then
            member = false
          end
        end
      end
      if member then
        found[#found + 1] = name
      end
    end
    return found, key
  end
  local response = ctx.http.request({
    url = ctx.origin .. "/api/oauth/usage",
    headers = { ["anthropic-beta"] = "oauth-2025-04-20" }
  })
  if response.status ~= 200 then
    error("usage endpoint status")
  end
  local payload = ctx.json.decode(response.body)
  if type(payload) ~= "table" or type(payload.limits) ~= "table" then
    error("usage payload has no limits")
  end
  local windows = {}
  for _, entry in ipairs(payload.limits) do
    if type(entry) ~= "table" then
      error("usage entry malformed")
    end
    local lane = nil
    local window = "seven_day"
    local bound = models
    if entry.kind == "session" or entry.kind == "weekly_all" then
      if present(entry.scope) then
        error("usage scope unknown")
      end
      if entry.kind == "session" then
        lane = "primary"
        window = "five_hour"
      else
        lane = "secondary"
      end
    elseif entry.kind == "weekly_scoped" then
      local key = nil
      bound, key = scoped(entry.scope)
      lane = "model:" .. key
    else
      error("usage kind unknown")
    end
    local percent = entry.percent
    if type(percent) ~= "number" or percent ~= percent or percent < 0 or percent > 100 then
      error("usage percent invalid")
    end
    if present(entry.is_active) and type(entry.is_active) ~= "boolean" then
      error("usage entry malformed")
    end
    local reset_at = nil
    if present(entry.resets_at) then
      if type(entry.resets_at) ~= "string" then
        error("usage reset invalid")
      end
      reset_at = ctx.time.rfc3339(entry.resets_at)
      if reset_at == nil then
        error("usage reset invalid")
      end
    end
    if #bound > 0 then
      windows[#windows + 1] = {
        pool = lane,
        window = window,
        models = bound,
        remaining_percent = 100.0 - percent,
        reset_at = reset_at,
        observed_at = ctx.now
      }
    end
  end
  if #windows == 0 then
    error("usage payload has no applicable limit")
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
/// First-party collectors contribute their frozen bytes and placement, and a
/// first-party identity with a script file is a typed conflict rather than a
/// silent override. A custom identity runs the retained revision of its
/// configured script file under its complete source identity (script id plus
/// canonical file path): the file is read through a bound checked before any
/// allocation beyond it, its bytes are installed only when they verify and
/// compile, and each accepted revision is also kept at
/// `home/capacity/scripts/<sha256(identity)>.lua`. A missing, oversized, or
/// invalid replacement — in this process or a later polling round — falls
/// back to that last valid revision; with none, the round fails typed.
/// Writing the durable copy is best effort: a failed write only shortens the
/// retention to this process.
fn resolve_script(
    home: &Path,
    unit: &Planned,
) -> std::result::Result<(String, AuthPlacement), CollectorError> {
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
    let cap = limits.vm_memory_bytes.min(MAX_SCRIPT_BYTES);
    let canonical = std::fs::canonicalize(&file).unwrap_or(file.clone());
    let identity = format!("{}\n{}", unit.script, canonical.display());
    let digest: String = Sha256::digest(identity.as_bytes())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    let retained = home.join("capacity/scripts").join(format!("{digest}.lua"));
    let mut registry = custom_registry()
        .lock()
        .map_err(|_| CollectorError::Internal("registry"))?;
    if let Some(text) = read_bounded(&file, cap) {
        if registry
            .install(&identity, CollectorScript::new(text.clone()), &limits)
            .is_ok()
            && crate::fs::private_dir(&home.join("capacity/scripts")).is_ok()
        {
            let _ = std::fs::write(&retained, text);
        }
    }
    if registry.get(&identity).is_none() {
        if let Some(text) = read_bounded(&retained, cap) {
            let _ = registry.install(&identity, CollectorScript::new(text), &limits);
        }
    }
    let script = registry
        .get(&identity)
        .ok_or(CollectorError::InvalidOutput(
            "collector_script_unavailable",
        ))?
        .source
        .clone();
    Ok((script, placement))
}

/// Largest accepted custom script, matching [`CollectorScript::verify`].
const MAX_SCRIPT_BYTES: usize = 256 * 1024;

/// Reads a UTF-8 regular file of at most `cap` bytes, allocating no more
/// than `cap + 1` bytes; returns `None` when absent, larger, or not UTF-8.
fn read_bounded(path: &Path, cap: usize) -> Option<String> {
    let file = std::fs::File::open(path).ok()?;
    if !file.metadata().ok()?.is_file() {
        return None;
    }
    let mut bytes = Vec::new();
    file.take(cap as u64 + 1).read_to_end(&mut bytes).ok()?;
    if bytes.len() > cap {
        return None;
    }
    String::from_utf8(bytes).ok()
}

/// Resolves one account's credential into an engine-held capability.
///
/// The account's own protected reference is read through `reader` at
/// request time only (for [`QuotaCredentialReader`], native/named Claude
/// logins resolve through their own harness store and Codex logins are
/// refused); the value lands in the redacted [`AuthCapability`] and nowhere
/// else. Callers must reduce any error to a fixed code: reader text may
/// echo credential material.
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
    AuthCapability::new(account.clone(), placement.clone(), value.into(), origins)
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
            match resolve_script(home, &unit) {
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
                                            windows = window_count(&snapshot);
                                            backoff.record_success(&unit.account, source);
                                        }
                                        Err(_) => issues.push(STORE_FAILED.to_owned()),
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
                        Err(_) => {
                            backoff.record_failure(&unit.account, source, at);
                            issues.push(CREDENTIAL_UNAVAILABLE.to_owned());
                        }
                    }
                }
            }
        }
        let status = row_status(&issues, windows);
        all_ok &= status != "failed";
        results.push(finish(entry, status, windows, issues));
    }
    Ok(json!({"ok": all_ok, "results": results}))
}

/// Stamps one unit's report row with its outcome facts.
pub(super) fn finish(mut entry: Value, status: &str, windows: usize, issues: Vec<String>) -> Value {
    entry["status"] = json!(status);
    entry["windows"] = json!(windows);
    entry["issues"] = json!(issues);
    entry
}

/// Counts the distinct physical `(pool, window)` observations persisted by
/// one snapshot; a window shared by several models counts once.
pub(super) fn window_count(snapshot: &agent_run_domain::catalog::NormalizedQuotaSnapshot) -> usize {
    snapshot
        .models
        .iter()
        .flat_map(|model| &model.pools)
        .flat_map(|pool| {
            pool.windows
                .iter()
                .map(move |window| (pool.key.as_str(), window.name.as_str()))
        })
        .collect::<BTreeSet<_>>()
        .len()
}

/// Classifies one unit outcome: any issue is `failed`, otherwise
/// `collected` when windows were persisted and `no_data` when none were.
pub(super) fn row_status(issues: &[String], windows: usize) -> &'static str {
    if !issues.is_empty() {
        "failed"
    } else if windows > 0 {
        "collected"
    } else {
        "no_data"
    }
}

/// One full account-scoped polling round for a v2 provider configuration.
///
/// Resolves the catalog against the registered account store, runs the Lua
/// collector units and the Codex app-server units, and persists the durable
/// backoff ledger so suppression survives the process boundary between
/// polling rounds. `ok` is true only when no row of either source failed and
/// the ledger persisted; a ledger write failure is reported as the fixed
/// `backoff_persist_failed` code. Report rows carry only static typed facts.
/// There is no legacy fallback: a failed source stays failed.
pub async fn collect_providers(home: &Path, config: &ProviderConfig) -> Result<Value> {
    let store = agent_run_store::Store::open(home)
        .map_err(|_| Error::Runtime("account registry is unavailable".into()))?;
    let catalog = config.resolve_catalog(store.list_accounts()?)?;
    let limits = CollectorLimits::default();
    let client = Arc::new(
        crate::capacity::lua::ReqwestQuotaHttp::new(limits.http_response_body_bytes)
            .map_err(|_| Error::Runtime("quota transport unavailable".into()))?,
    );
    let claude_home = config
        .harnesses
        .get(&agent_run_domain::catalog::HarnessId::ClaudeCode)
        .ok_or_else(|| crate::error::invalid("v2 requires the claude-code harness"))?
        .home
        .clone();
    let reader = QuotaCredentialReader::from_host(home.to_path_buf(), claude_home)
        .ok_or_else(|| Error::Runtime("HOME is unavailable".into()))?;
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
    let codex =
        super::codex_quota::codex_appserver_round(home, config, &catalog, &mut backoff, retention)
            .await?;
    let mut ok = report["ok"].as_bool().unwrap_or(false)
        && codex.iter().all(|row| row["status"] != "failed");
    if backoff.save(home, now()).is_err() {
        report["backoff_persist"] = json!(BACKOFF_PERSIST_FAILED);
        ok = false;
    }
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
