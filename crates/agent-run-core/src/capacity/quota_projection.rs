//! Pure schema-2 read projections from Quota facts and one local registry snapshot.
//!
//! Callers fetch one validated snapshot before opening the local read transaction.
//! A missing response stays unknown; these helpers never collect or forecast quota.

use super::{provider_catalog, provider_ranking, quota_adapter};
use crate::{Result, quota_client, state::Store};
use agent_run_config::provider_config::ProviderConfig;
use agent_run_domain::{
    CapacityOrderQuery, ModelsQuery,
    catalog::{ProviderCatalog, ProviderDefinition, ProviderModel},
};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
};

/// Preserves the existing structured provider catalog over one Quota response.
/// `snapshot` must have come from the validating Quota client; `None` means outage.
pub fn models_at(
    store: &Store,
    home: &Path,
    config: &ProviderConfig,
    revision: &str,
    query: &ModelsQuery,
    snapshot: Option<&quota_client::Snapshot>,
    at: f64,
) -> Result<Value> {
    let _read = begin_read(store)?;
    let mut incomplete = false;
    let mut value = provider_catalog::models_from_store(
        store,
        home,
        config,
        revision,
        query,
        at,
        |store, catalog, keep, clock| {
            let (order, missing) = prepared_order(store, catalog, keep, snapshot, clock)?;
            incomplete = missing;
            Ok(order)
        },
    )?;
    value["quota_coverage"] = coverage(snapshot, incomplete);
    Ok(value)
}

/// Preserves the existing account-free provider order over one Quota response.
pub fn order_at(
    store: &Store,
    config: &ProviderConfig,
    revision: &str,
    query: &CapacityOrderQuery,
    snapshot: Option<&quota_client::Snapshot>,
    at: f64,
) -> Result<Value> {
    let _read = begin_read(store)?;
    let catalog = provider_catalog::resolve(config, store)?;
    provider_catalog::check_model(&catalog, query.model.as_deref())?;
    let (order, incomplete) = prepared_order(
        store,
        &catalog,
        &|_, offering| {
            query
                .model
                .as_deref()
                .is_none_or(|wanted| wanted == offering.id)
        },
        snapshot,
        at,
    )?;
    Ok(order_with_coverage(&order, revision, snapshot, incomplete))
}

/// Preserves `limits` items and ranking, adding a nonsecret coverage marker.
/// Incomplete or absent quota is never represented as a confirmed empty result.
pub fn limits_at(
    store: &Store,
    config: &ProviderConfig,
    revision: &str,
    snapshot: Option<&quota_client::Snapshot>,
    at: f64,
) -> Result<Value> {
    let _read = begin_read(store)?;
    let catalog = provider_catalog::resolve(config, store)?;
    let (order, incomplete_models) = prepared_order(store, &catalog, &|_, _| true, snapshot, at)?;
    let bound: BTreeSet<_> = catalog
        .providers()
        .iter()
        .flat_map(|provider| {
            provider
                .bindings
                .iter()
                .map(|binding| binding.account.as_str())
        })
        .collect();
    // Raw limits cover every configured binding, even when local admission
    // excludes it. Quota's complete collection only covers its own roster.
    let missing_bound = snapshot.is_some_and(|source| {
        let returned: BTreeSet<_> = source
            .accounts
            .iter()
            .map(|view| view.account.as_str())
            .collect();
        !bound.is_subset(&returned)
    });
    let mut rows = Vec::new();
    if let Some(snapshot) = snapshot {
        for account in &snapshot.accounts {
            if bound.contains(account.account.as_str()) {
                rows.extend(account.windows.iter().map(|window| (account, window)));
            }
        }
    }
    rows.sort_by(|(a, left), (b, right)| {
        (
            &left.runtime,
            &left.lane,
            &left.window,
            &left.target,
            &left.source,
            &a.account,
            &left.pool,
        )
            .cmp(&(
                &right.runtime,
                &right.lane,
                &right.window,
                &right.target,
                &right.source,
                &b.account,
                &right.pool,
            ))
    });
    let truncated = rows.len() > 1000;
    rows.truncate(1000);
    let mut incomplete_windows = false;
    let items: Vec<Value> = rows.into_iter().map(|(account, window)| {
        let source_clock = snapshot.is_some_and(|s| at >= s.evaluated_at);
        let known = source_clock && window.forecast.known && window.forecast.remaining_percent.is_some()
            && window.observed_at.is_some_and(|observed| observed <= at)
                && window.valid_until.is_some_and(|until| at <= until)
            && window.last_reset_at.is_none_or(|reset| at < reset);
        incomplete_windows |= !known;
        json!({
            "key":{"runtime":window.runtime,"lane":window.lane,"window":window.window,
                   "target":window.target,"source":window.source},
            "known":known,
            "remaining_percent":if known {window.forecast.remaining_percent} else {None},
            "reset_at":window.last_reset_at,"observed_at":window.observed_at,"valid_until":window.valid_until,
            "account":account.account,"pool":window.pool,
        })
    }).collect();
    let coverage = coverage(
        snapshot,
        truncated || missing_bound || incomplete_models || incomplete_windows,
    );
    Ok(json!({
        "observed_at":at,
        "items":items,
        "ranking":order_with_coverage(&order, revision, snapshot, incomplete_models),
        "coverage":coverage,
    }))
}

/// Adds nonsecret source-clock metadata to the existing structured order.
fn order_with_coverage(
    order: &provider_ranking::ProviderCapacityOrder,
    revision: &str,
    snapshot: Option<&quota_client::Snapshot>,
    incomplete: bool,
) -> Value {
    let mut value = provider_catalog::order_value(order, revision);
    value["quota_coverage"] = coverage(snapshot, incomplete);
    value
}

/// Projects the compact account-free guide from the same models view.
pub fn delegation_guide_at(
    store: &Store,
    home: &Path,
    config: &ProviderConfig,
    revision: &str,
    query: &ModelsQuery,
    snapshot: Option<&quota_client::Snapshot>,
    at: f64,
) -> Result<Value> {
    let catalog = models_at(store, home, config, revision, query, snapshot, at)?;
    Ok(Value::String(crate::delegation_guide::render(&catalog)?))
}

/// Joins an existing local read transaction, or starts a short one for this projection.
fn begin_read<'a>(store: &'a Store<'_>) -> Result<Option<rusqlite::Transaction<'a>>> {
    if store.conn.is_autocommit() {
        Ok(Some(store.conn.unchecked_transaction()?))
    } else {
        Ok(None)
    }
}

/// Applies existing local ordering to Quota's own state and score without forecasting.
fn prepared_order(
    store: &Store,
    catalog: &ProviderCatalog,
    keep: &dyn Fn(&ProviderDefinition, &ProviderModel) -> bool,
    snapshot: Option<&quota_client::Snapshot>,
    at: f64,
) -> Result<(provider_ranking::ProviderCapacityOrder, bool)> {
    let (order, mut incomplete) = provider_ranking::provider_order_prepared_at(
        store,
        catalog,
        &BTreeSet::new(),
        keep,
        at,
        |accounts, definition, offering, clock| {
            let mut fence = quota_adapter::StateFence::default();
            quota_adapter::score(
                accounts,
                &definition.id,
                offering,
                snapshot,
                clock,
                &mut fence,
                None,
            )
        },
        |account, _, definition, offering| {
            let source = snapshot.filter(|source| at >= source.evaluated_at)?;
            quota_adapter::matching_model(Some(source), account, &definition.id, offering)?
                .newest_observed_at
                .filter(|observed| observed.is_finite() && *observed <= at)
        },
    )?;
    // Coverage describes the requested catalog, not just the subset currently
    // eligible for admission. Do not let disabled or cooled-down bindings hide
    // a missing, unknown, or expired Quota class from that marker.
    for definition in catalog.providers() {
        for offering in definition
            .models
            .iter()
            .filter(|model| keep(definition, model))
        {
            let accounts: BTreeMap<_, _> = definition
                .bindings
                .iter()
                .filter(|binding| {
                    binding
                        .models
                        .as_ref()
                        .is_none_or(|models| models.contains(&offering.id))
                })
                .map(|binding| (binding.account.clone(), 1.0))
                .collect();
            let mut fence = quota_adapter::StateFence::default();
            let classes = quota_adapter::score(
                &accounts,
                &definition.id,
                offering,
                snapshot,
                at,
                &mut fence,
                None,
            )?;
            incomplete |= classes.iter().any(|(_, verdict, _)| {
                matches!(
                    verdict,
                    provider_ranking::LaneVerdict::Unknown
                        | provider_ranking::LaneVerdict::Overflow
                )
            });
        }
    }
    Ok((order, incomplete))
}

/// Separates Quota's collection coverage from incomplete local projection evidence.
fn coverage(snapshot: Option<&quota_client::Snapshot>, incomplete: bool) -> Value {
    let (state, evaluated_at) = match snapshot {
        None => ("unknown", None),
        Some(snapshot) => (
            match snapshot.coverage.state {
                quota_client::CoverageState::Complete => "complete",
                quota_client::CoverageState::Partial => "partial",
                quota_client::CoverageState::Unknown => "unknown",
            },
            Some(snapshot.evaluated_at),
        ),
    };
    json!({"state":state,"incomplete":incomplete || state != "complete", "evaluated_at":evaluated_at})
}

/// Refuses legacy route order; the owner must explicitly migrate configuration.
pub fn legacy_order_unavailable() -> Result<Value> {
    Err(crate::Error::Unsupported(
        "legacy quota order requires migration to schema_version 2".into(),
    ))
}

/// Refuses legacy limits because their local history is no longer authoritative.
pub fn legacy_limits_unavailable() -> Result<Value> {
    Err(crate::Error::Unsupported(
        "legacy limits require migration to schema_version 2".into(),
    ))
}
