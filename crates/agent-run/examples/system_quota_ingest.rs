//! Bridge account-owned quota probes to the broker's existing quota writer.

use std::collections::{BTreeMap, BTreeSet};
use std::io::{self, Read};
use std::path::Path;
use std::time::Duration;

use agent_run_config::provider_config::ProviderConfig;
use agent_run_core::capacity::quota::{
    CollectorScope, MAX_OUTPUT_MODELS, MAX_OUTPUT_WINDOWS, normalize_collector_output,
};
use agent_run_core::{domain::now, persist};
use agent_run_domain::catalog::{AccountId, AccountStatus, ProviderId};
use agent_run_domain::{CredentialRef, Error, Result};
use agent_run_store::{Store, ops::RecordQuotaSnapshot};
use serde_json::{Value, json};

/// Resolves one enabled binding and its allowed native model lanes from live state.
fn context(home: &Path, provider: &str, account: &str) -> Result<(Value, CollectorScope, usize)> {
    let (config, revision) = ProviderConfig::load(home)?;
    let catalog = config.resolve_catalog(Store::open(home)?.list_accounts()?)?;
    let provider_id: ProviderId = provider.parse()?;
    let account_id: AccountId = account.parse()?;
    let definition = catalog
        .provider(&provider_id)
        .ok_or_else(|| Error::Validation("unknown quota provider".into()))?;
    let record = catalog
        .account(&account_id)
        .filter(|account| account.status == AccountStatus::Enabled)
        .ok_or_else(|| Error::Validation("unknown or disabled quota account".into()))?;
    let binding = definition
        .bindings
        .iter()
        .find(|binding| binding.account == account_id)
        .ok_or_else(|| Error::Validation("quota account is not bound to provider".into()))?;
    let harness = config
        .harnesses
        .get(&definition.harness)
        .ok_or_else(|| Error::Validation("quota harness is not configured".into()))?;
    let auth = match CredentialRef::from_secret(&record.secret_ref)? {
        CredentialRef::Native(agent_run_domain::catalog::HarnessId::Codex) => {
            json!({"kind": "codex_native"})
        }
        CredentialRef::Named {
            harness: agent_run_domain::catalog::HarnessId::ClaudeCode,
            label,
        } => json!({"kind": "claude_named", "label": label.as_str()}),
        _ => return Err(Error::Validation("unsupported quota account type".into())),
    };
    let mut models = BTreeMap::new();
    for model in &definition.models {
        if binding
            .models
            .as_ref()
            .is_some_and(|ids| !ids.contains(&model.id))
        {
            continue;
        }
        let lane = model.native_model.as_ref().unwrap_or(&model.id).clone();
        models.insert(lane.clone(), json!({"id": model.id, "native_model": lane}));
    }
    if models.is_empty() {
        return Err(Error::Validation("quota binding has no models".into()));
    }
    let source = match (provider, auth["kind"].as_str()) {
        ("codex", Some("codex_native")) => "codex-appserver",
        ("claude", Some("claude_named")) => "anthropic-usage",
        _ => {
            return Err(Error::Validation(
                "unsupported quota provider binding".into(),
            ));
        }
    };
    let scope = CollectorScope {
        runtime: provider.to_owned(),
        source: source.to_owned(),
        models: models.keys().cloned().collect::<BTreeSet<_>>(),
    };
    Ok((
        json!({"version": 1, "models": models, "now": now(), "auth": auth,
            "fence": {"config": revision, "account": agent_run_platform::fs::sha256(record.secret_ref.as_str().as_bytes())},
            "harness": {"id": definition.harness, "command": harness.binary}}),
        scope,
        config.capacity.sample_retention,
    ))
}

/// Reads one bounded JSON document from an account-owned collector's stdout.
fn read_observation() -> Result<Value> {
    const LIMIT: u64 = 2 * 1024 * 1024;
    let mut bytes = Vec::new();
    io::stdin().lock().take(LIMIT + 1).read_to_end(&mut bytes)?;
    if bytes.len() as u64 > LIMIT {
        return Err(Error::Validation("quota observation is too large".into()));
    }
    serde_json::from_slice(&bytes)
        .map_err(|_| Error::Validation("invalid quota observation".into()))
}

/// Refuses a probe whose configuration or registered account changed in flight.
fn verified_observation<'a>(context: &Value, response: &'a Value) -> Result<&'a Value> {
    let object = response
        .as_object()
        .filter(|object| object.len() == 2)
        .ok_or_else(|| Error::Validation("invalid quota response envelope".into()))?;
    if object.get("fence") != context.get("fence") {
        return Err(Error::Validation(
            "quota account or configuration changed".into(),
        ));
    }
    object
        .get("observation")
        .ok_or_else(|| Error::Validation("quota observation is missing".into()))
}

/// Validates one fenced probe and sends its account-bound facts to the broker.
async fn persist_observation(
    home: &Path,
    context: &Value,
    scope: CollectorScope,
    retention: usize,
    account: &AccountId,
    response: &Value,
) -> Result<()> {
    let raw = verified_observation(context, response)?;
    let snapshot = normalize_collector_output(
        account,
        &scope,
        raw,
        now(),
        MAX_OUTPUT_WINDOWS,
        MAX_OUTPUT_MODELS,
    )?;
    if snapshot.models.is_empty() {
        return Err(Error::Validation("quota observation is empty".into()));
    }
    persist::collector_producer(home)
        .call(
            RecordQuotaSnapshot {
                runtime: scope.runtime,
                snapshot,
                retention,
                at: now(),
            },
            Duration::from_secs(5),
        )
        .await?;
    Ok(())
}

/// Emits a nonsecret probe context or validates and persists its observation.
async fn run() -> Result<()> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    let [mode, home, provider, account] = args.as_slice() else {
        return Err(Error::Validation(
            "expected MODE HOME PROVIDER ACCOUNT".into(),
        ));
    };
    let home = Path::new(home);
    let (context, scope, retention) = context(home, provider, account)?;
    match mode.as_str() {
        "context" => println!("{context}"),
        "ingest" => {
            let response = read_observation()?;
            let account_id: AccountId = account.parse()?;
            persist_observation(home, &context, scope, retention, &account_id, &response).await?;
            println!("{{\"stored\":true}}");
        }
        _ => return Err(Error::Validation("unknown quota operation".into())),
    }
    Ok(())
}

/// The input may include account metadata, so errors never echo its contents.
#[tokio::main(flavor = "current_thread")]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("quota integration failed: {error}");
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use agent_run_domain::catalog::{AccountRecord, AuthFamily};

    /// Creates an isolated two-provider registry without real credentials.
    fn fixture() -> tempfile::TempDir {
        let home = tempfile::tempdir().unwrap();
        agent_run_store::fixture::initialize(home.path()).unwrap();
        let mut store = agent_run_store::fixture::open(home.path()).unwrap();
        for (id, family, reference) in [
            ("codex-native", "openai", "native:codex"),
            ("claude-api", "anthropic", "env:FAKE_API_KEY"),
            (
                "claude-subscription",
                "anthropic",
                "named:claude-code:subscription",
            ),
        ] {
            store
                .register_account(&AccountRecord {
                    account_id: id.parse().unwrap(),
                    auth_family: family.parse::<AuthFamily>().unwrap(),
                    secret_ref: reference.parse().unwrap(),
                    status: AccountStatus::Enabled,
                })
                .unwrap();
        }
        std::fs::write(
            home.path().join("config.toml"),
            format!(
                r#"schema_version = 2
[harnesses.codex]
binary = "/bin/true"
home = "{home}/codex"
[harnesses.claude-code]
binary = "/bin/true"
home = "{home}/claude"
[providers.codex]
harness = "codex"
connection = {{ kind = "native" }}
auth_family = "openai"
limits_source = "none"
[[providers.codex.models]]
id = "codex-model"
[[providers.codex.bindings]]
label = "native"
account = "codex-native"
[providers.claude]
harness = "claude-code"
connection = {{ kind = "custom", endpoint = "https://example.com", protocol = "messages" }}
auth_family = "anthropic"
limits_source = "none"
ordered_accounts = true
[[providers.claude.models]]
id = "claude-model"
[[providers.claude.bindings]]
label = "api"
account = "claude-api"
[[providers.claude.bindings]]
label = "subscription"
account = "claude-subscription"
connection = {{ kind = "native" }}
priority_multiplier = 0.5
"#,
                home = home.path().display()
            ),
        )
        .unwrap();
        home
    }

    /// Account scope and native model membership come only from live config.
    #[test]
    fn context_resolves_only_the_requested_bound_subscription() {
        let home = fixture();
        let (codex, codex_scope, _) = context(home.path(), "codex", "codex-native").unwrap();
        assert_eq!(codex["auth"]["kind"], "codex_native");
        assert!(codex_scope.models.contains("codex-model"));
        let (claude, claude_scope, _) =
            context(home.path(), "claude", "claude-subscription").unwrap();
        assert_eq!(claude["auth"]["kind"], "claude_named");
        assert_eq!(claude["auth"]["label"], "subscription");
        assert!(claude_scope.models.contains("claude-model"));
        assert!(context(home.path(), "claude", "claude-api").is_err());
        assert!(context(home.path(), "codex", "claude-subscription").is_err());
        assert!(context(home.path(), "claude", "not-registered").is_err());
    }

    /// A result is not rebound after an account or config revision changes.
    #[test]
    fn quota_response_requires_the_original_binding_fence() {
        let context = json!({"fence":{"config":"a","account":"b"}});
        let valid =
            json!({"fence":{"config":"a","account":"b"},"observation":{"version":1,"windows":[]}});
        assert!(verified_observation(&context, &valid).is_ok());
        let changed =
            json!({"fence":{"config":"a","account":"c"},"observation":{"version":1,"windows":[]}});
        assert!(verified_observation(&context, &changed).is_err());
        let unbound = json!({"observation":{"version":1,"windows":[]}});
        assert!(verified_observation(&context, &unbound).is_err());
    }
}
