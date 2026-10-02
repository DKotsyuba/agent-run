//! Native cache lifecycle glue between the supervisor and the native units.
//!
//! This module owns no storage of its own: `native_cache` publishes the two
//! Codex metadata caches, `native_tree_cache` freezes and thaws the generated
//! skills tree and remote plugin parents, and the coordinator anchors the
//! physical home. What lives here is the lifecycle the supervisor and the
//! operator surfaces share:
//!
//! * the trusted compatibility domain — derived from the frozen harness and
//!   native settings, the frozen provider connection and the internal
//!   selected account identity, never an account label and never a token —
//!   that keeps one account's native cache from being restored for another;
//! * the pre-launch step, which recovers this home's own native journals and
//!   thaws every remote plugin parent before any native invocation that could
//!   mutate the cache, including probes, explicit resumes and the
//!   account-switch loop;
//! * the idle consolidation, which anchors the home in the layout registry
//!   (so a cache-only home with no managed roots stays in the collector's
//!   census), freezes eligible native trees and packs the metadata caches.
//!
//! Every step is best-effort by contract: the native caches are rebuildable
//! cold-start data, so a failure is reported and skipped, never allowed to
//! fail a valid model answer, erase source data, or block a managed-asset
//! resume. Nothing here rewrites the frozen index, history, credentials or
//! authority digests.
use crate::{Result, domain::AgentId, fs, service::ProviderLaunchIdentity, state::Store};
use agent_run_domain::{AccountId, catalog::HarnessId, error::invalid};
use serde::Serialize;
use std::path::{Path, PathBuf};

/// Home-relative root of the generated system skills tree.
const SYSTEM_SKILLS: &str = "skills/.system";
/// Home-relative parent of native plugin caches.
const PLUGIN_CACHE: &str = "plugins/cache";
/// The one managed marketplace, never a native cache candidate.
const MANAGED_MARKET: &str = "personal";

/// The outcome of one idle consolidation over one exclusively owned home.
#[derive(Default, Debug, Serialize)]
pub struct CacheConsolidation {
    /// Native roots captured into the shared store this pass.
    pub frozen: usize,
    /// Native roots already shared and re-verified unchanged.
    pub already_frozen: usize,
    /// Roots examined and left private: absent markers, unsupported shapes.
    pub skipped: usize,
    /// Metadata-cache entries published and linked.
    pub packed: usize,
    /// Metadata-cache entries already shared and re-verified.
    pub already_shared: usize,
    /// Metadata-cache entries retained private, by disposition.
    pub retained: usize,
    /// True when any bounded pass or an unreadable home ended the work early.
    pub bounded: bool,
}

/// Returns the trusted native compatibility domain of one frozen execution
/// and its actually selected internal account identity.
///
/// The preimage is the frozen harness identity, the frozen harness's native
/// settings, the frozen provider connection (endpoint, protocol and header
/// style) and the internal account id — never a mutable label, never a
/// credential, and never the account's token material, which this module
/// cannot even see. Identical content under different domains never
/// converges, so one account's native cache is never restored for another;
/// the effective uid keeps users separate the same way the metadata cache's
/// own scope already does.
pub fn native_domain(identity: &ProviderLaunchIdentity, account: &AccountId) -> Result<String> {
    let authority = &identity.authority;
    let config = &identity.provider_config;
    let mut preimage = format!(
        "agent-run/native-cache/v1\n{}\n",
        serde_json::to_value(authority.harness)
            .map_err(|_| invalid("harness identity is not serializable"))?
            .as_str()
            .unwrap_or_default()
    );
    // Incomplete frozen evidence is a refusal, never a guessed domain: a
    // missing harness or provider entry would silently narrow the preimage
    // and could converge one account's cache onto another's.
    let harness = config
        .harnesses
        .get(&authority.harness)
        .ok_or_else(|| invalid("frozen harness is unavailable for the native domain"))?;
    preimage.push_str(
        &String::from_utf8(fs::canonical_json(&serde_json::to_value(
            &harness.native_settings,
        )?)?)
        .map_err(|_| invalid("native settings are not canonical text"))?,
    );
    let provider = config
        .providers
        .get(&authority.provider)
        .ok_or_else(|| invalid("frozen provider is unavailable for the native domain"))?;
    preimage.push('\n');
    preimage.push_str(
        &String::from_utf8(fs::canonical_json(&serde_json::to_value(
            &provider.connection,
        )?)?)
        .map_err(|_| invalid("provider connection is not canonical text"))?,
    );
    preimage.push('\n');
    preimage.push_str(account.as_str());
    // SAFETY: geteuid only reads kernel credential state and retains nothing.
    preimage.push_str(&format!("\n{}", unsafe { libc::geteuid() }));
    Ok(fs::sha256(preimage.as_bytes()))
}

/// Recovers this home's own interrupted native operations and thaws every
/// frozen remote plugin parent and curated clone root, before a native
/// invocation that could write.
///
/// The native store rewrites remote plugin parents in place and the native
/// curated sync fetches into, stages and activates the curated clone, so a
/// frozen parent, curated working tree or Git pack directory must be private
/// again before any child exists: this runs before
/// every attempt, including probes, explicit resumes and the account-switch
/// loop. The generated skills tree keeps its link — native discovery reads
/// through it and a marker upgrade replaces the private link itself.
///
/// `retained` distinguishes the two ways a home can be absent: a genuinely
/// new execution whose home is not sealed yet has nothing to recover, while a
/// home a frozen identity already recorded must exist — its absence is an
/// error, never a silent empty pass. Only a plain `NotFound` store root means
/// nothing was ever shared; everything else that cannot be read — a corrupt
/// or aliased store root, permission, I/O, a foreign ancestor — propagates,
/// so the caller refuses to launch shared data unprotected instead of falling
/// back silently.
pub fn prepare_native(app_home: &Path, runtime_home: &Path, retained: bool) -> Result<usize> {
    let root = crate::runtime_storage::store_root(app_home)?;
    match std::fs::symlink_metadata(&root) {
        // Nothing is shared yet: no native publication ever ran.
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(error) => return Err(error.into()),
        // `store_root` already proved an existing root a real directory.
        Ok(_) => {}
    }
    match fs::Dir::open(runtime_home) {
        Ok(_) => {}
        Err(error) if not_found(&error) => {
            if retained {
                return Err(invalid(
                    "a sealed runtime home is missing; recover it before launching",
                ));
            }
            // A fresh execution's home does not exist until it is sealed.
            return Ok(0);
        }
        Err(error) => return Err(error),
    }
    crate::native_tree_cache::recover(&root, runtime_home)?;
    let mut thawed = 0;
    let mut roots = curated_roots(runtime_home)?;
    roots.extend(plugin_parents(runtime_home)?);
    for root_key in roots {
        if crate::native_tree_cache::thaw(&root, runtime_home, &root_key)?.is_some() {
            thawed += 1;
        }
    }
    Ok(thawed)
}

/// Lists the curated clone roots present in one home right now.
///
/// A plain `NotFound` anywhere on either root's path means that root is
/// absent; any other error — an unreadable or linked ancestor — propagates,
/// so uncertainty about a frozen root never becomes a silent skip.
fn curated_roots(runtime_home: &Path) -> Result<Vec<String>> {
    let home = fs::Dir::open(runtime_home)?;
    let mut roots = Vec::new();
    for root_key in [
        crate::native_tree_cache::CURATED_MIRROR_ROOT,
        crate::native_tree_cache::CURATED_PACK_ROOT,
    ] {
        match home.entry_type(Path::new(root_key)) {
            Ok(_) => roots.push(root_key.to_owned()),
            Err(error) if not_found(&error) => {}
            Err(error) => return Err(error),
        }
    }
    Ok(roots)
}

/// Returns `true` only for a plain `NotFound`.
fn not_found(error: &agent_run_domain::Error) -> bool {
    matches!(
        error,
        agent_run_domain::Error::Io(inner)
            if inner.kind() == std::io::ErrorKind::NotFound
    )
}

/// Returns `true` for an entry that can never be a frozen parent or its
/// market: a regular file such as native metadata, or a name that raced away.
///
/// A directory, a link or a special entry returns `false` so the caller
/// classifies it or fails on it; any other error propagates.
fn ordinary_file(directory: &fs::Dir, name: &Path) -> Result<bool> {
    match directory.entry_type(name) {
        Ok(fs::EntryType::File) => Ok(true),
        Ok(_) => Ok(false),
        Err(error) if not_found(&error) => Ok(true),
        Err(error) => Err(error),
    }
}

/// Lists the home-relative remote plugin parents that exist right now.
///
/// Only immediate `<market>/<plugin>` children of the native plugin cache
/// that classify as remote plugin parents are candidates: an ordinary private
/// directory this system never froze — including names outside the safe
/// identity charset — is left untouched and never blocks a launch. The
/// managed `personal` marketplace is never a candidate: its parents are
/// managed plugin views, not native caches, and an ordinary regular file at
/// either level is native metadata, never a parent. Only a plain `NotFound` means
/// "nothing here": an unreadable home, cache, market or any other error is
/// returned, because uncertainty about a frozen parent must not become a
/// silent skip.
fn plugin_parents(runtime_home: &Path) -> Result<Vec<String>> {
    let home = match fs::Dir::open(runtime_home) {
        Ok(home) => home,
        Err(error) if not_found(&error) => return Ok(Vec::new()),
        Err(error) => return Err(error),
    };
    let cache = match home.subdir(Path::new(PLUGIN_CACHE)) {
        Ok(cache) => cache,
        Err(error) if not_found(&error) => return Ok(Vec::new()),
        Err(error) => return Err(error),
    };
    let mut roots = Vec::new();
    for market in cache.list(None)? {
        let market = market.to_string_lossy().into_owned();
        if market == MANAGED_MARKET || ordinary_file(&cache, Path::new(&market))? {
            continue;
        }
        let market_dir = match cache.subdir(Path::new(&market)) {
            // A listed name that no longer opens as a directory raced away.
            Err(error) if not_found(&error) => continue,
            Err(error) => return Err(error),
            Ok(directory) => directory,
        };
        for plugin in market_dir.list(None)? {
            if ordinary_file(&market_dir, Path::new(&plugin))? {
                continue;
            }
            let root_key = format!("{PLUGIN_CACHE}/{market}/{}", plugin.to_string_lossy());
            // Only a proven remote-plugin parent is a thaw candidate; an
            // unsupported or unsafe private name stays exactly as it is.
            if matches!(
                crate::native_tree_cache::classify(&root_key),
                Ok(crate::native_tree_cache::NativeCacheKind::RemotePluginParent)
            ) {
                roots.push(root_key);
            }
        }
    }
    Ok(roots)
}

/// Consolidates one exclusively owned, quiescent home's native caches.
///
/// The caller proves exclusivity and quiescence — confirmed cleanup before
/// the terminal state is committed, or the operator's offline locks over an
/// all-terminal lineage — so no native process can hold these trees. The home
/// is first anchored in the layout registry (a cache-only home has no managed
/// roots to map; the row is what keeps it in the collector's census after its
/// agent history expires), then eligible native trees — the system skills,
/// the curated clone's working tree and Git packs, and remote plugin
/// parents — are frozen and both
/// metadata caches packed under the caller-supplied compatibility domain.
/// Every step is reported and skipped on failure: an optional cache never
/// turns a valid answer into a failure, and a failed preparation with a
/// journal stays recoverable by the next entry or the operator.
pub fn consolidate(
    store: &mut Store,
    id: &AgentId,
    identity: &ProviderLaunchIdentity,
    account: &AccountId,
    app_home: &Path,
    runtime_home: &Path,
    qualified: &Path,
) -> Result<CacheConsolidation> {
    let mut report = CacheConsolidation::default();
    if identity.authority.harness != HarnessId::Codex {
        // Only the Codex harness has the native caches this glue knows.
        return Ok(report);
    }
    let root = crate::runtime_storage::store_root(app_home)?;
    if root != qualified {
        return Err(invalid(
            "native cache publication requires the qualified shared store root",
        ));
    }
    crate::native_tree_cache::recover(&root, runtime_home)?;
    // The anchor precedes any publication: from here on the registry names
    // this physical home, whatever its agent rows later become.
    crate::runtime_storage::anchor(
        store,
        app_home,
        runtime_home,
        identity.authority.assets_sha256.as_str(),
        Some(id),
    )?;
    let scope = native_domain(identity, account)?;
    let mut roots = vec![SYSTEM_SKILLS.to_owned()];
    roots.extend(curated_roots(runtime_home)?);
    roots.extend(plugin_parents(runtime_home)?);
    for root_key in roots {
        match crate::native_tree_cache::freeze(&root, runtime_home, &root_key, &scope) {
            Ok(crate::native_tree_cache::FreezeOutcome::Frozen(_)) => report.frozen += 1,
            Ok(crate::native_tree_cache::FreezeOutcome::AlreadyFrozen(_)) => {
                report.already_frozen += 1
            }
            Ok(crate::native_tree_cache::FreezeOutcome::SkippedUnchanged) => report.skipped += 1,
            Err(error) => {
                // Best effort by contract: report and continue.
                if let Some(logger) = crate::logging::configured() {
                    logger.log(
                        crate::logging::Level::Debug,
                        &format!("native cache freeze skipped: {}", error.public().kind),
                    );
                }
                report.skipped += 1;
            }
        }
    }
    for kind in [
        crate::native_cache::NativeCacheKind::Tools,
        crate::native_cache::NativeCacheKind::ServerInfo,
    ] {
        let domain = crate::native_cache::NativeCacheDomain::new(kind, &scope)?;
        let packed = crate::native_cache::pack_native_cache(&root, runtime_home, &domain)?;
        report.bounded |= !packed.complete;
        for entry in &packed.entries {
            match entry.disposition {
                crate::native_cache::NativeCacheDisposition::Packed { .. } => report.packed += 1,
                crate::native_cache::NativeCacheDisposition::AlreadyShared => {
                    report.already_shared += 1
                }
                _ => report.retained += 1,
            }
        }
    }
    Ok(report)
}

/// Returns whether one home still holds any shared native or managed link.
///
/// The launch path uses this to decide whether a home whose managed map is
/// empty still launches behind the shared-store guard: a cache-only home
/// reads through its native links, so the store must stay provably outside
/// every root the child can write even when no managed root is mapped.
pub fn holds_shared_links(app_home: &Path, runtime_home: &Path) -> Result<bool> {
    if !crate::runtime_storage::store_root(app_home)
        .map(|root| root.is_dir())
        .unwrap_or(false)
    {
        return Ok(false);
    }
    let home = match fs::Dir::open(runtime_home) {
        Ok(home) => home,
        Err(_) => return Ok(false),
    };
    let mut candidates = vec![PathBuf::from(SYSTEM_SKILLS)];
    for root_key in plugin_parents(runtime_home)? {
        candidates.push(PathBuf::from(root_key));
    }
    for relative in candidates {
        // One no-follow stat per known cache root; anything linked there is
        // shared content this home reads through the store.
        if matches!(home.entry_type(&relative), Ok(fs::EntryType::Symlink)) {
            return Ok(true);
        }
    }
    // The metadata caches link individual files, not their directories.
    for dir in [
        crate::native_cache::TOOLS_CACHE_DIR,
        crate::native_cache::SERVER_INFO_CACHE_DIR,
    ] {
        let Some(cache) = home.subdir(Path::new(dir)).ok() else {
            continue;
        };
        for name in cache.list(None)? {
            if matches!(
                cache.entry_type(&PathBuf::from(&name)),
                Ok(fs::EntryType::Symlink)
            ) {
                return Ok(true);
            }
        }
    }
    Ok(false)
}
