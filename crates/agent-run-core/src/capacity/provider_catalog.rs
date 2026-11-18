//! Read-only provider catalog behind the delegation guide (schema 2).
//!
//! Projection combines one validated immutable config revision with one
//! committed local read. It never reads or presents quota, probes a harness,
//! reserves capacity, writes samples or starts agents; Agent Quota owns
//! capacity presentation. The orchestrator alone chooses provider, model,
//! effort and profile; this view reports configured facts, declared
//! recommendation prose and canonical role grants — no automatic choice or
//! score of model ability. No account id, label, or credential reference is
//! emitted.

use crate::{Result, error::invalid};
use agent_run_config::{policy, profiles, provider_config::ProviderConfig, role_plan};
use agent_run_domain::{
    ModelsQuery, ProviderStartRequest,
    catalog::{ProviderCatalog, ProviderConnection, ProviderDefinition, ProviderModel},
};
use agent_run_store::Store;
use serde_json::{Value, json};
use std::path::Path;

/// Loads the canonical role `name` exactly as provider admission does, using
/// a catalog-only request for `provider`/`model` (no task is admitted).
fn load_role(
    config: &ProviderConfig,
    home: &Path,
    provider: &str,
    model: &str,
    name: &str,
) -> Result<agent_run_config::profiles::Profile> {
    let request: ProviderStartRequest = serde_json::from_value(json!({
        "provider": provider, "model": model, "profile": name,
        "task": "catalog", "workdir": home,
    }))
    .map_err(|_| invalid("profile must be a configured name"))?;
    profiles::load_provider(config, &request)
}

/// Returns whether admission would accept `role` for this provider model:
/// the model's hard restrictions join the role's required constraints, the
/// role plan resolves against configured skills/MCP, and the effective
/// policy admits on the provider's harness.
fn admissible(
    config: &ProviderConfig,
    provider: &ProviderDefinition,
    offering: &ProviderModel,
    role: &agent_run_config::profiles::Profile,
) -> bool {
    let mut profile = role.clone();
    profile
        .required_constraints
        .extend(offering.restrictions.iter().copied());
    let native = offering.native_model.as_deref().unwrap_or(&offering.id);
    role_plan::resolve_role_plan(&profile, config.skills_dir(), &config.mcp, "global", None).is_ok()
        && agent_run_adapters::provider::runtime(config, provider.harness, native).is_ok_and(
            |runtime| {
                agent_run_adapters::validate_role(&runtime, &profile).is_ok()
                    && policy::evaluate(provider.id.as_str(), &runtime, &profile)
                        .admit()
                        .is_ok()
            },
        )
}

/// Lists canonical role names in the configured profile directory, sorted.
fn role_names(config: &ProviderConfig) -> Vec<String> {
    let mut names: Vec<String> = std::fs::read_dir(config.profiles_dir())
        .map(|entries| {
            entries
                .filter_map(|entry| entry.ok())
                .filter_map(|entry| {
                    entry
                        .file_name()
                        .to_str()
                        .and_then(|name| name.strip_suffix(".md"))
                        .filter(|name| crate::config::name(name))
                        .map(str::to_owned)
                })
                .collect()
        })
        .unwrap_or_default();
    names.sort();
    names
}

/// Renders a connection without endpoints' credentials (there are none).
fn connection(connection: &ProviderConnection) -> Value {
    match connection {
        ProviderConnection::Native => json!({"kind": "native"}),
        ProviderConnection::Custom { protocol, .. } => {
            json!({"kind": "custom", "protocol": protocol})
        }
    }
}

/// Resolves the catalog against the committed account registry.
pub(super) fn resolve(config: &ProviderConfig, store: &Store) -> Result<ProviderCatalog> {
    config.resolve_catalog(store.list_accounts()?)
}

/// Validates `model` against the catalog: it must be offered somewhere.
pub(super) fn check_model(catalog: &ProviderCatalog, model: Option<&str>) -> Result<()> {
    if let Some(model) = model
        && !catalog
            .providers()
            .iter()
            .any(|provider| provider.models.iter().any(|offering| offering.id == model))
    {
        return Err(invalid("model filter names no configured offering"));
    }
    Ok(())
}

/// Projects the account-free provider catalog for the delegation guide from
/// one committed local read: providers and models in configured order, each
/// model's params, restrictions, recommendations and admissible canonical
/// roles. Carries no quota standing, ranking or account identity; Agent Quota
/// owns capacity presentation. Exact filters are validated as before.
pub fn catalog_at(
    store: &Store,
    home: &Path,
    config: &ProviderConfig,
    revision: &str,
    query: &ModelsQuery,
) -> Result<Value> {
    let _read = if store.conn.is_autocommit() {
        Some(store.conn.unchecked_transaction()?)
    } else {
        None
    };
    let catalog = resolve(config, store)?;
    if let Some(provider) = &query.provider
        && catalog.provider(&provider.parse()?).is_none()
    {
        return Err(invalid("provider filter names no configured provider"));
    }
    check_model(&catalog, query.model.as_deref())?;
    let any = catalog
        .providers()
        .iter()
        .flat_map(|provider| {
            provider
                .models
                .iter()
                .map(move |offering| (provider.id.as_str(), offering.id.as_str()))
        })
        .next();
    // An explicitly empty catalog offers nothing, so no role is admissible.
    let names = match (&query.profile, any) {
        (Some(_), None) => return Err(invalid("profile filter names no canonical role")),
        (_, None) => vec![],
        (Some(name), Some(_)) => vec![name.clone()],
        (None, Some(_)) => role_names(config),
    };
    let any = any.unwrap_or_default();
    let mut roles = Vec::new();
    for name in names {
        match load_role(config, home, any.0, any.1, &name) {
            Ok(profile) => roles.push(profile),
            Err(_) if query.profile.is_some() => {
                return Err(invalid("profile filter names no canonical role"));
            }
            Err(_) => {}
        }
    }
    let mut providers = Vec::new();
    for definition in catalog.providers() {
        if query
            .provider
            .as_deref()
            .is_some_and(|wanted| wanted != definition.id.as_str())
        {
            continue;
        }
        let mut models = Vec::new();
        for offering in &definition.models {
            if query
                .model
                .as_deref()
                .is_some_and(|wanted| wanted != offering.id)
            {
                continue;
            }
            let admissible: Vec<&str> = roles
                .iter()
                .filter(|role| admissible(config, definition, offering, role))
                .map(|role| role.name.as_str())
                .collect();
            // A profile filter keeps only offerings every named role admits.
            if query.profile.is_some() && admissible.len() != roles.len() {
                continue;
            }
            models.push(json!({
                "model": offering.id,
                "native_model": offering.native_model,
                "params": offering.params,
                "allowed_params": offering.allowed_params,
                "restrictions": offering.restrictions,
                "recommendations": offering.recommendations,
                "profiles": admissible,
            }));
        }
        if models.is_empty() {
            continue;
        }
        providers.push(json!({
            "provider": definition.id,
            "harness": definition.harness,
            "connection": connection(&definition.connection),
            "auth_family": definition.auth_family,
            "recommendations": definition.recommendations,
            "models": models,
        }));
    }
    Ok(json!({
        "schema_version": 2,
        "config_revision": revision,
        "providers": providers,
    }))
}

/// Renders the compact account-free delegation guide from [`catalog_at`].
pub fn delegation_guide_at(
    store: &Store,
    home: &Path,
    config: &ProviderConfig,
    revision: &str,
    query: &ModelsQuery,
) -> Result<Value> {
    let catalog = catalog_at(store, home, config, revision, query)?;
    Ok(Value::String(crate::delegation_guide::render(&catalog)?))
}
