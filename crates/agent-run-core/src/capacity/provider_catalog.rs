//! Read-only public provider catalog and provider capacity order (schema 2).
//!
//! Both reads combine one validated immutable config revision with one
//! committed quota/registry read. They never collect quota, probe a harness,
//! reserve capacity, consume reset credits, write samples, or start agents.
//! The orchestrator alone chooses provider, model, effort and profile; these
//! views report configured facts, declared recommendation prose, canonical
//! role grants, and cached quota standing — no automatic choice or score of
//! model ability. No account id, label, or credential reference is emitted.

use crate::{capacity::provider_ranking, error::invalid, Result};
use agent_run_config::{policy, profiles, provider_config::ProviderConfig, role_plan};
use agent_run_domain::{
    catalog::{ProviderCatalog, ProviderConnection, ProviderDefinition, ProviderModel},
    CapacityOrderQuery, ModelsQuery, ProviderStartRequest,
};
use agent_run_store::Store;
use serde_json::{json, Value};
use std::{collections::BTreeSet, path::Path};

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
                policy::evaluate(provider.id.as_str(), &runtime, &profile)
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
fn resolve(config: &ProviderConfig, store: &Store) -> Result<ProviderCatalog> {
    config.resolve_catalog(store.list_accounts()?)
}

/// Validates `model` against the catalog: it must be offered somewhere.
fn check_model(catalog: &ProviderCatalog, model: Option<&str>) -> Result<()> {
    if let Some(model) = model {
        if !catalog
            .providers()
            .iter()
            .any(|provider| provider.models.iter().any(|offering| offering.id == model))
        {
            return Err(invalid("model filter names no configured offering"));
        }
    }
    Ok(())
}

/// The public provider catalog (`models`) for one config revision.
///
/// `revision` is the exact-byte SHA-256 of the cached `config`. The result
/// carries `config_revision`, the committed `capacity_revision` of its one
/// quota read, a `roles_sha256` digest of the listed role grants, the
/// canonical `profiles`, and `providers` in provider capacity order, each with
/// harness, connection kind, recommendations, and every offered model's
/// native alias, default `params`, `allowed_params`, hard `restrictions`,
/// recommendations, admissible `profiles`, and cached `quota` standing
/// (`available`/`unknown`/`priority_overflow`/`exhausted`/
/// `no_eligible_account`). `unknown` means configured but not observed; it
/// is not a live-health claim. Filters are exact (see [`ModelsQuery`]);
/// unknown provider/profile/model values are validation errors.
pub fn models(
    home: &Path,
    config: &ProviderConfig,
    revision: &str,
    query: &ModelsQuery,
) -> Result<Value> {
    let store = Store::open(home)?;
    let catalog = resolve(config, &store)?;
    if let Some(provider) = &query.provider {
        if catalog.provider(&provider.parse()?).is_none() {
            return Err(invalid("provider filter names no configured provider"));
        }
    }
    check_model(&catalog, query.model.as_deref())?;
    let order = provider_ranking::provider_order_for_model_at(
        &store,
        &catalog,
        &BTreeSet::new(),
        query.model.as_deref(),
        crate::domain::now(),
    )?;
    let any = catalog
        .providers()
        .iter()
        .flat_map(|provider| {
            provider
                .models
                .iter()
                .map(move |offering| (provider.id.as_str(), offering.id.as_str()))
        })
        .next()
        .ok_or_else(|| invalid("no provider offers a model"))?;
    let names = match &query.profile {
        Some(name) => vec![name.clone()],
        None => role_names(config),
    };
    let mut roles = Vec::new();
    let mut profiles_out = Vec::new();
    for name in names {
        match load_role(config, home, any.0, any.1, &name) {
            Ok(profile) => {
                profiles_out.push(json!({
                    "name": profile.name,
                    "revision": profile.revision,
                    "write": profile.write,
                    "network": profile.network,
                    "allow_external_read_roots": profile.allow_external_read_roots,
                    "read_roots": profile.read_roots,
                    "required_constraints": profile.required_constraints,
                    "skills": profile.skills,
                    "mcp": profile.mcp,
                }));
                roles.push(profile);
            }
            Err(_) if query.profile.is_some() => {
                return Err(invalid("profile filter names no canonical role"));
            }
            Err(_) => profiles_out.push(json!({"name": name, "canonical": false})),
        }
    }
    let mut providers = Vec::new();
    for entry in &order.providers {
        if query
            .provider
            .as_deref()
            .is_some_and(|wanted| wanted != entry.provider.as_str())
        {
            continue;
        }
        let definition = catalog
            .provider(&entry.provider)
            .ok_or_else(|| invalid("provider order names an unknown provider"))?;
        let mut models = Vec::new();
        for standing in &entry.models {
            let offering = definition
                .models
                .iter()
                .find(|offering| offering.id == standing.model)
                .ok_or_else(|| invalid("provider order names an unknown model"))?;
            let admissible: Vec<&str> = roles
                .iter()
                .filter(|role| admissible(config, definition, offering, role))
                .map(|role| role.name.as_str())
                .collect();
            if query.profile.is_some() && admissible.is_empty() {
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
                "quota": {"status": standing.status, "best_priority": standing.best_priority},
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
            "limits_source": definition.limits_source,
            "priority_multiplier": entry.priority_multiplier,
            "score": entry.score,
            "recommendations": definition.recommendations,
            "models": models,
        }));
    }
    Ok(json!({
        "schema_version": 2,
        "config_revision": revision,
        "capacity_revision": order.capacity_revision,
        "observed_at": order.observed_at,
        "roles_sha256": agent_run_domain::canonical::sha256_hex(&json!(profiles_out), true),
        "profiles": profiles_out,
        "providers": providers,
    }))
}

/// The public provider-only capacity order for one config revision.
///
/// Providers, never provider/account pairs, in descending score with each
/// offered model's own status and best priority; with `query.model` only
/// providers offering that model appear, ranked by that model. Carries the
/// `config_revision` and committed `capacity_revision` of its one read.
pub fn order(
    home: &Path,
    config: &ProviderConfig,
    revision: &str,
    query: &CapacityOrderQuery,
) -> Result<Value> {
    let store = Store::open(home)?;
    let catalog = resolve(config, &store)?;
    check_model(&catalog, query.model.as_deref())?;
    let order = provider_ranking::provider_order_for_model_at(
        &store,
        &catalog,
        &BTreeSet::new(),
        query.model.as_deref(),
        crate::domain::now(),
    )?;
    Ok(json!({
        "schema_version": 2,
        "config_revision": revision,
        "capacity_revision": order.capacity_revision,
        "observed_at": order.observed_at,
        "providers": order.providers,
    }))
}
