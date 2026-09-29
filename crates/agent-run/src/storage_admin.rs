//! Operator storage administration: survey, offline compaction, recovery.
//!
//! `agent-run storage status` reports what the shared managed-asset store and
//! the retained runtime homes actually hold, from durable evidence only: the
//! state schema, the configured run roots, every frozen identity's recorded
//! home, and the layout registry. It measures unique inode bytes with a
//! bounded walk and reports the filesystem's own free space separately, so a
//! measured logical saving is never presented as a physical one. Partial
//! scans are labelled incomplete instead of silently reported as totals.
//!
//! `storage compact` plans the same survey read-only; `--apply` additionally
//! holds the broker and service-manager startup locks — the same exclusion a
//! paired config migration uses — refuses while agents are active, relocates
//! each eligible retained home behind the real supervisor guard preflight,
//! and then runs one reference-aware collection pass over the shared store.
//! A home whose recorded harness, workdir, binary or grants cannot be
//! verified is skipped with its reason and preserved byte count, never forced
//! through a rewritten configuration.
//!
//! `storage recover` rolls interrupted relocations forward under the same
//! offline locks: it finishes only rows this home provably owns, resuming the
//! exact operation token, references and staged backups, and refuses foreign
//! or tampered state explicitly. No command here starts a model turn; the
//! only child processes are the launch preflight's metadata-only probes, and
//! only under `--apply`.

use crate::{migrate, Result};
use agent_run_core::{fs, runtime_storage, state::Store, storage_gc};
use agent_run_domain::error::invalid;
use serde_json::{json, Value};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};

/// Upper bound on homes one survey reports per page.
const HOMES_PER_PAGE: usize = 256;
/// Upper bound on store entries measured for the unique-byte total.
const STORE_MEASURE_ENTRIES: usize = 20_000;
/// Upper bound on entries measured inside one runtime home.
const HOME_MEASURE_ENTRIES: usize = 8_192;
/// Upper bound on pending rows one recover pass finishes.
const RECOVER_ROWS: usize = 1_000;

/// One surveyed runtime home's classification and measured sizes.
struct Surveyed {
    /// Canonical runtime home exactly as durable evidence records it.
    home: String,
    /// `shared`, `eligible`, `prepared`, `protected`, `gone` or `unknown`.
    state: &'static str,
    /// How many recorded executions bind this home, resume lineage included.
    holders: usize,
    /// Statuses of holders that are still active or lost, if any.
    active: Vec<String>,
    /// Latest terminal execution, the one whose seal qualifies the home.
    executor: Option<String>,
    /// Finish time of that execution, for ordering.
    sealed_at: f64,
    /// Sealed asset-index digest recorded for the home, when known.
    index_sha256: Option<String>,
    /// Unique inode bytes of the home's private (non-shared) content.
    private_bytes: u64,
    /// True when a measurement or verification bound was hit.
    incomplete: bool,
    /// Human-readable reasons for the classification, when it is not `shared`.
    reasons: Vec<String>,
}

/// One survey's public report plus the internal executors it selected.
struct Survey {
    /// The operator-facing JSON report.
    report: Value,
    /// Latest terminal execution per eligible home, internal use only.
    executors: Vec<(String, String)>,
}

/// Measures unique inode bytes below one directory, never following symlinks.
///
/// Returns the total and whether the entry budget ran out, so a partial
/// measurement is always labelled instead of reported as exact.
fn unique_bytes(directory: &Path, budget: usize) -> (u64, bool) {
    fn walk(dir: &fs::Dir, seen: &mut BTreeMap<u64, ()>, total: &mut u64, budget: &mut usize) {
        let names = match dir.list(None) {
            Ok(names) => names,
            Err(_) => return,
        };
        for name in names {
            if *budget == 0 {
                return;
            }
            let relative = PathBuf::from(&name);
            let Ok(entry) = dir.entry(Some(&relative)) else {
                continue;
            };
            *budget -= 1;
            match entry.kind {
                fs::EntryType::Directory => {
                    if let Ok(child) = dir.subdir(&relative) {
                        walk(&child, seen, total, budget);
                    }
                }
                fs::EntryType::File => {
                    if seen.contains_key(&entry.inode) {
                        continue;
                    }
                    if let Ok(file) = dir.open_file(&relative) {
                        if let Ok(metadata) = file.metadata() {
                            seen.insert(entry.inode, ());
                            *total += metadata.len();
                        }
                    }
                }
                _ => continue,
            }
        }
    }
    let mut total = 0_u64;
    let mut budget = budget;
    let mut seen = BTreeMap::new();
    if let Ok(dir) = fs::Dir::open(directory) {
        walk(&dir, &mut seen, &mut total, &mut budget);
    }
    (total, budget == 0)
}

/// Returns the `runs` directories of the configured harness homes: the
/// configured run roots inside which an unrecorded home could still exist.
fn configured_run_roots(home: &Path) -> Vec<PathBuf> {
    let Ok(bytes) = std::fs::read(home.join("config.toml")) else {
        return Vec::new();
    };
    let Ok(text) = std::str::from_utf8(&bytes) else {
        return Vec::new();
    };
    let Ok(config) = agent_run_config::provider_config::ProviderConfig::parse(text, home) else {
        return Vec::new();
    };
    config
        .harnesses
        .values()
        .map(|harness| harness.home.join("runs"))
        .collect()
}

/// Lists the entry names of one existing directory, else an empty list.
fn child_names(root: &Path) -> Vec<String> {
    let Ok(dir) = fs::Dir::open(root) else {
        return Vec::new();
    };
    dir.list(None)
        .unwrap_or_default()
        .into_iter()
        .filter_map(|name| name.to_str().map(str::to_owned))
        .collect()
}

/// Surveys every retained runtime home and the shared store, read-only.
///
/// Homes come only from durable identities and configured run roots, and all
/// holders of one home are considered together: a resume lineage legitimately
/// leaves many terminal executions sharing one home, so a home is protected
/// only while some holder is active or lost, and qualification uses the
/// latest terminal holder's sealed index. A home no identity records is
/// reported as unknown and never touched, and so are aliases and unreadable
/// homes; a home is `gone` only on a plain `NotFound`. Registry errors are
/// reported, never hidden behind a fallback. An unmeasurable or partially
/// measured home sets `incomplete` on the whole report rather than passing a
/// partial number off as a total.
fn survey(home: &Path, store: &Store) -> Result<Survey> {
    let store_root = runtime_storage::store_root(home)?;
    let store_present = store_root.exists();
    let mut homes: BTreeMap<String, Surveyed> = BTreeMap::new();
    let mut incomplete = false;
    let mut after: Option<String> = None;
    let mut more = true;
    while more {
        let (page, remaining) = store.retained_runtime_homes(after.as_deref(), HOMES_PER_PAGE)?;
        more = remaining;
        for reference in page {
            after = Some(reference.runtime_home.clone());
            let entry = homes
                .entry(reference.runtime_home.clone())
                .or_insert_with(|| Surveyed {
                    home: reference.runtime_home.clone(),
                    state: "unknown",
                    holders: 0,
                    active: Vec::new(),
                    executor: None,
                    sealed_at: 0.0,
                    index_sha256: None,
                    private_bytes: 0,
                    incomplete: false,
                    reasons: Vec::new(),
                });
            entry.holders += 1;
            let unsettled = agent_run_store::ACTIVE_SQL.contains(reference.status.as_str())
                || reference.status == "lost";
            if unsettled {
                entry.active.push(reference.status.clone());
            } else {
                // The latest terminal execution is the one whose sealed
                // authority and native history qualify the home.
                let sealed = reference.finished_at.unwrap_or(0.0);
                if entry.executor.is_none() || sealed >= entry.sealed_at {
                    entry.sealed_at = sealed;
                    entry.executor = Some(reference.agent_id.clone());
                    if reference.index_sha256.is_some() {
                        entry.index_sha256 = reference.index_sha256.clone();
                    }
                }
            }
        }
    }
    // Configured run roots contribute homes no identity records. They are
    // reported and never touched: nothing durable proves what they are.
    for runs in configured_run_roots(home) {
        for name in child_names(&runs) {
            let Ok(canonical) = runs.join(&name).canonicalize() else {
                continue;
            };
            let key = canonical.to_string_lossy().into_owned();
            homes.entry(key.clone()).or_insert(Surveyed {
                home: key,
                state: "unknown",
                holders: 0,
                active: Vec::new(),
                executor: None,
                sealed_at: 0.0,
                index_sha256: None,
                private_bytes: 0,
                incomplete: false,
                reasons: vec!["no durable identity records this home".into()],
            });
        }
    }
    let mut totals: BTreeMap<&str, u64> = BTreeMap::new();
    let mut counted: BTreeMap<&str, u64> = BTreeMap::new();
    for entry in homes.values_mut() {
        let path = PathBuf::from(&entry.home);
        // The registry keys the canonical path; an identity may record an
        // unresolved spelling of the same home, so the lookup canonicalizes
        // rather than silently missing its row. A row that fails its own
        // digest check is reported, never treated as absent.
        let canonical = path
            .canonicalize()
            .map(|resolved| resolved.to_string_lossy().into_owned())
            .unwrap_or_else(|_| entry.home.clone());
        let layout = match store.runtime_storage_layout(&canonical) {
            Ok(layout) => layout,
            Err(error) => {
                entry.state = "unknown";
                entry.incomplete = true;
                incomplete = true;
                entry.reasons.push(error.to_string());
                continue;
            }
        };
        if !entry.active.is_empty() {
            entry.state = "protected";
            entry.reasons.push(format!(
                "{} holder(s) of this home are still active or lost",
                entry.active.len()
            ));
        }
        match fs::Dir::open(&path) {
            Err(error)
                if matches!(&error, agent_run_domain::Error::Io(inner)
                    if inner.kind() == std::io::ErrorKind::NotFound) =>
            {
                entry.state = "gone";
                entry.reasons.push("physical home is gone".into());
                *counted.entry("gone").or_default() += 1;
                continue;
            }
            Err(error) => {
                // Unreadable is not gone: the home and its row are kept.
                entry.state = "unknown";
                entry.incomplete = true;
                incomplete = true;
                entry.reasons.push(error.to_string());
                continue;
            }
            Ok(_) => {}
        }
        match layout.as_ref().map(|record| record.state) {
            Some(agent_run_store::runtime_storage::LayoutState::Prepared) => {
                entry.state = "prepared";
                entry.reasons.push(
                    "an interrupted relocation holds a prepared layout; run `storage recover`"
                        .into(),
                );
            }
            Some(agent_run_store::runtime_storage::LayoutState::Committed) => {
                entry.state = "shared";
                entry.reasons.clear();
            }
            None => {
                if entry.state != "protected" {
                    // Eligibility is proven only by the read-only planner: the
                    // original index must still strictly verify in place.
                    let digest = entry.index_sha256.clone().unwrap_or_default();
                    match runtime_storage::plan(store, home, &path, &digest, &shared_scope()) {
                        Ok(Some(_)) => entry.state = "eligible",
                        Ok(None) => {
                            // A cache-only home maps no managed root: managed
                            // relocation is a no-op, and native consolidation
                            // still anchors it and shares its caches.
                            entry.state = "eligible";
                            entry
                                .reasons
                                .push("no managed roots; native caches only".into());
                        }
                        Err(error) => {
                            entry.state = "unknown";
                            entry.reasons.push(error.to_string());
                        }
                    }
                }
            }
        }
        let (bytes, hit) = unique_bytes(&path, HOME_MEASURE_ENTRIES);
        entry.private_bytes = bytes;
        entry.incomplete = hit;
        incomplete |= hit;
        *totals.entry(entry.state).or_default() += bytes;
        *counted.entry(entry.state).or_default() += 1;
    }
    let (store_bytes, store_incomplete) = if store_present {
        unique_bytes(&store_root, STORE_MEASURE_ENTRIES)
    } else {
        (0, false)
    };
    // The filesystem's own free space is reported separately from the unique
    // inode bytes above: clones, snapshots and hardlinks mean reclaiming an
    // inode does not have to move this number, and no fake physical saving is
    // claimed from a logical one.
    let free = fs2::available_space(if store_present { &store_root } else { home }).unwrap_or(0);
    let entries: Vec<Value> = homes
        .values()
        .map(|entry| {
            json!({
                "runtime_home": entry.home,
                "state": entry.state,
                "holders": entry.holders,
                "index_sha256": entry.index_sha256,
                "private_bytes": entry.private_bytes,
                "measured": !entry.incomplete,
                "reasons": entry.reasons,
            })
        })
        .collect();
    let states = ["eligible", "shared", "protected", "prepared", "unknown"];
    let mut totals_json = json!({});
    for state in states {
        totals_json[&format!("{state}_homes")] = json!(counted.get(state).copied().unwrap_or(0));
        totals_json[&format!("{state}_bytes")] = json!(totals.get(state).copied().unwrap_or(0));
    }
    Ok(Survey {
        report: json!({
            "state_schema_version": agent_run_store::VERSION,
            "store_root": store_root,
            "store_present": store_present,
            "store_unique_bytes": store_bytes,
            "store_measured": !store_incomplete,
            "filesystem_free_bytes": free,
            "incomplete": incomplete,
            "homes": {
                "total": homes.len(),
                "gone": counted.get("gone").copied().unwrap_or(0),
                "incomplete": incomplete,
                "entries": entries,
            },
            "totals": totals_json,
        }),
        // Internal only: per-run executor ids never leave the process, so the
        // public report carries no migrating execution identifiers.
        executors: homes
            .values()
            .filter(|entry| entry.state == "eligible")
            .filter_map(|entry| {
                entry
                    .executor
                    .clone()
                    .map(|agent| (entry.home.clone(), agent))
            })
            .collect(),
    })
}

/// `agent-run storage status`: a read-only report; never a store write.
pub fn status(home: &Path) -> Result<Value> {
    migrate::require_current_store(home)?;
    let store = Store::open(home)?;
    Ok(survey(home, &store)?.report)
}

/// Refuses while any agent is active, matching the migration boundary.
fn require_idle(store: &Store) -> Result<()> {
    let active: i64 = store.conn.query_row(
        &format!(
            "SELECT COUNT(*) FROM agents WHERE status IN {}",
            agent_run_store::ACTIVE_SQL
        ),
        [],
        |row| row.get(0),
    )?;
    if active > 0 {
        return Err(invalid(
            "active agents must finish before offline storage work",
        ));
    }
    Ok(())
}

/// The managed scope the launch path derives for this user's shared store.
fn shared_scope() -> String {
    agent_run_core::supervisor::managed_scope()
}

/// `agent-run storage compact [--apply]`: plan read-only, or relocate and
/// collect offline.
///
/// The dry run writes nothing anywhere: it reuses the survey and the
/// collection preview, both of which only read schema, configuration,
/// identities and the store's directory entries. `--apply` first takes the
/// broker and service-manager startup locks and refuses active agents, then
/// relocates each eligible single-holder home behind the real guard preflight
/// and runs one collecting pass.
pub fn compact(home: &Path, apply: bool) -> Result<Value> {
    migrate::require_current_store(home)?;
    let mut store = Store::open(home)?;
    if !apply {
        let plan = survey(home, &store)?;
        let collection = storage_gc::sweep(&mut store, home, storage_gc::Mode::Preview)?;
        return Ok(json!({
            "applied": false,
            "plan": plan.report,
            "collect": collection_report(&collection),
        }));
    }
    let _locks = migrate::broker_exclusion(home)?;
    require_idle(&store)?;
    let plan = survey(home, &store)?;
    // Each eligible home relocates once, behind its latest terminal
    // execution's recorded authority; internal per-run ids stay here.
    let mut eligible = plan.executors.clone();
    eligible.sort();
    let mut relocations = Vec::new();
    for (home_path, agent) in &eligible {
        let agent = match agent.parse() {
            Ok(agent) => agent,
            Err(_) => {
                relocations.push(json!({
                    "runtime_home": home_path,
                    "relocated": false,
                    "reason": "recorded agent id is malformed",
                }));
                continue;
            }
        };
        let outcome = agent_run_core::supervisor::relocate_retained_home(&mut store, home, &agent);
        let result = match outcome {
            Ok(agent_run_core::supervisor::Relocation::Installed) => {
                json!({"runtime_home": home_path, "relocated": true})
            }
            Ok(agent_run_core::supervisor::Relocation::AlreadyShared) => json!({
                "runtime_home": home_path,
                "relocated": false,
                "reason": "no managed roots or already shared",
            }),
            Ok(agent_run_core::supervisor::Relocation::Skipped {
                reason,
                private_bytes,
            }) => json!({
                "runtime_home": home_path,
                "relocated": false,
                "reason": reason,
                "preserved_private_bytes": private_bytes,
            }),
            Err(error) => json!({
                "runtime_home": home_path,
                "relocated": false,
                "reason": error.to_string(),
            }),
        };
        relocations.push(result);
    }
    // Native caches consolidate under the same offline locks and the same
    // latest-terminal-executor authority, for every eligible home whether or
    // not it maps managed roots.
    let mut native = Vec::new();
    for (home_path, agent) in &eligible {
        let agent = match agent.parse() {
            Ok(agent) => agent,
            Err(_) => continue,
        };
        let result =
            match agent_run_core::supervisor::consolidate_retained_native(&mut store, home, &agent)
            {
                Ok(report) => json!({"runtime_home": home_path, "consolidated": report}),
                Err(reason) => {
                    json!({"runtime_home": home_path, "consolidated": false, "reason": reason})
                }
            };
        native.push(result);
    }
    let collection = storage_gc::sweep(&mut store, home, storage_gc::Mode::Apply)?;
    Ok(json!({
        "applied": true,
        "plan": plan.report,
        "relocations": relocations,
        "native": native,
        "collect": collection_report(&collection),
    }))
}

/// `agent-run storage recover`: roll interrupted relocations forward offline.
///
/// Only rows this home provably owns are finished, from their own recorded
/// token, references and staged backups; a foreign or tampered home is
/// reported as refused and left exactly as found. Recovery never starts a
/// model and never rolls anything back: unsupported rollback stays a separate
/// explicit operation.
pub fn recover(home: &Path) -> Result<Value> {
    migrate::require_current_store(home)?;
    let _locks = migrate::broker_exclusion(home)?;
    let mut store = Store::open(home)?;
    require_idle(&store)?;
    // Interrupted native operations recover alongside the managed
    // coordinator: every extant registered home's own journals complete or
    // roll back from their records, before anything is reported.
    let mut native_recovered = 0;
    let mut native_refused = 0;
    if let Ok(root) = runtime_storage::store_root(home) {
        if root.is_dir() {
            let mut after: Option<String> = None;
            loop {
                let Ok((page, more)) =
                    store.runtime_storage_layouts_page(after.as_deref(), RECOVER_ROWS)
                else {
                    native_refused += 1;
                    break;
                };
                for record in &page {
                    after = Some(record.runtime_home.clone());
                    let path = PathBuf::from(&record.runtime_home);
                    if fs::Dir::open(&path).is_err() {
                        continue;
                    }
                    match agent_run_core::native_tree_cache::recover(&root, &path) {
                        Ok(()) => native_recovered += 1,
                        Err(_) => native_refused += 1,
                    }
                }
                if !more {
                    break;
                }
            }
        }
    }
    let mut outcomes = Vec::new();
    for record in store.pending_runtime_storage_layouts(RECOVER_ROWS)? {
        let path = PathBuf::from(&record.runtime_home);
        let result = match runtime_storage::recover(&mut store, home, &path) {
            Ok(()) => json!({"runtime_home": record.runtime_home, "recovered": true}),
            Err(error) => json!({
                "runtime_home": record.runtime_home,
                "recovered": false,
                "reason": error.to_string(),
            }),
        };
        outcomes.push(result);
    }
    Ok(json!({
        "applied": true,
        "recovered": outcomes
            .iter()
            .filter(|entry| entry["recovered"] == json!(true))
            .count(),
        "refused": outcomes
            .iter()
            .filter(|entry| entry["recovered"] == json!(false))
            .count(),
        "native_recovered": native_recovered,
        "native_refused": native_refused,
        "outcomes": outcomes,
    }))
}

/// Renders one collection outcome for the operator surface.
fn collection_report(outcome: &storage_gc::Outcome) -> Value {
    json!({
        "trees_removed": outcome.trees_removed,
        "blobs_removed": outcome.blobs_removed,
        "views_removed": outcome.views_removed,
        "staging_removed": outcome.staging_removed,
        "rows_removed": outcome.rows_removed,
        "bytes_reclaimed": outcome.bytes_reclaimed,
        "trees_retained": outcome.trees_retained,
        "blobs_retained": outcome.blobs_retained,
        "lock_busy": outcome.lock_busy,
        "incomplete": outcome.incomplete,
        "backlog": outcome.backlog(),
    })
}
