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
/// `revision` is the exact-byte SHA-256 of the cached, immutable `config`;
/// role files are read once and digested into `roles_sha256`. Every mutable
/// fact — registry status, samples, exhaustion latches, and the advertised
/// `capacity_revision` — comes from ONE committed read transaction, so a
/// registry or quota change between reads can never be mixed into one
/// result. `ranked_at` is the advice clock the standing was evaluated at,
/// not a sample age; each offering's `quota` carries `status`,
/// `best_priority`, `evidence` (`fresh`/`stale`/`missing`),
/// `newest_observed_at` (sample age), and `exhausted_until`. Providers are
/// ranked over exactly the offerings the filters retain (see
/// [`provider_ranking::provider_order_filtered_at`]), so a removed offering
/// never lends its score. Filters are exact (see [`ModelsQuery`]); unknown
/// provider/profile/model values are validation errors.
pub fn models(
    home: &Path,
    config: &ProviderConfig,
    revision: &str,
    query: &ModelsQuery,
) -> Result<Value> {
    models_between(home, config, revision, query, &mut || {})
}

/// [`models`] with a hook between the registry read and the quota read of
/// its one transaction (feature `test-fixtures`): a test seam proving a
/// concurrent registry or quota commit there cannot mix into the result.
#[cfg(feature = "test-fixtures")]
pub fn models_observed(
    home: &Path,
    config: &ProviderConfig,
    revision: &str,
    query: &ModelsQuery,
    between_reads: &mut dyn FnMut(),
) -> Result<Value> {
    models_between(home, config, revision, query, between_reads)
}

/// Shared body of [`models`]; `between_reads` runs after the registry read
/// and before the quota read, inside the same read transaction.
fn models_between(
    home: &Path,
    config: &ProviderConfig,
    revision: &str,
    query: &ModelsQuery,
    between_reads: &mut dyn FnMut(),
) -> Result<Value> {
    let store = Store::open(home)?;
    // One committed read: registry, samples, latches and revision together.
    let _read = store.conn.unchecked_transaction()?;
    let catalog = resolve(config, &store)?;
    if let Some(provider) = &query.provider {
        if catalog.provider(&provider.parse()?).is_none() {
            return Err(invalid("provider filter names no configured provider"));
        }
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
    between_reads();
    let keep = |definition: &ProviderDefinition, offering: &ProviderModel| {
        query
            .provider
            .as_deref()
            .is_none_or(|wanted| wanted == definition.id.as_str())
            && query
                .model
                .as_deref()
                .is_none_or(|wanted| wanted == offering.id)
            && (query.profile.is_none()
                || roles
                    .iter()
                    .all(|role| admissible(config, definition, offering, role)))
    };
    let order = provider_ranking::provider_order_filtered_at(
        &store,
        &catalog,
        &BTreeSet::new(),
        &keep,
        crate::domain::now(),
    )?;
    let mut providers = Vec::new();
    for entry in &order.providers {
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
            models.push(json!({
                "model": offering.id,
                "native_model": offering.native_model,
                "params": offering.params,
                "allowed_params": offering.allowed_params,
                "restrictions": offering.restrictions,
                "recommendations": offering.recommendations,
                "profiles": admissible,
                "quota": quota(standing),
            }));
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
        "ranked_at": order.observed_at,
        "roles_sha256": agent_run_domain::canonical::sha256_hex(&json!(profiles_out), true),
        "profiles": profiles_out,
        "providers": providers,
    }))
}

/// Renders one offering's cached standing without any account identity.
fn quota(standing: &provider_ranking::ProviderModelOrder) -> Value {
    json!({
        "status": standing.status,
        "best_priority": standing.best_priority,
        "evidence": standing.evidence,
        "newest_observed_at": standing.newest_observed_at,
        "exhausted_until": standing.exhausted_until,
    })
}

/// The public provider-only capacity order for one config revision.
///
/// Providers, never provider/account pairs, in descending score with each
/// offered model's own standing (as in [`models`]); with `query.model` only
/// providers offering that model appear, ranked by that model. Registry and
/// quota facts and the advertised `capacity_revision` come from one
/// committed read; `ranked_at` is the advice clock.
pub fn order(
    home: &Path,
    config: &ProviderConfig,
    revision: &str,
    query: &CapacityOrderQuery,
) -> Result<Value> {
    let store = Store::open(home)?;
    let _read = store.conn.unchecked_transaction()?;
    let catalog = resolve(config, &store)?;
    check_model(&catalog, query.model.as_deref())?;
    let order = provider_ranking::provider_order_filtered_at(
        &store,
        &catalog,
        &BTreeSet::new(),
        &|_, offering| {
            query
                .model
                .as_deref()
                .is_none_or(|wanted| wanted == offering.id)
        },
        crate::domain::now(),
    )?;
    let providers: Vec<Value> = order
        .providers
        .iter()
        .map(|entry| {
            json!({
                "provider": entry.provider,
                "priority_multiplier": entry.priority_multiplier,
                "score": entry.score,
                "models": entry.models.iter().map(|standing| json!({
                    "model": standing.model,
                    "native_model": standing.native_model,
                    "quota": quota(standing),
                })).collect::<Vec<_>>(),
            })
        })
        .collect();
    Ok(json!({
        "schema_version": 2,
        "config_revision": revision,
        "capacity_revision": order.capacity_revision,
        "ranked_at": order.observed_at,
        "providers": providers,
    }))
}
