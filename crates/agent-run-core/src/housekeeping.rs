//! Bounded filesystem retention for recognized disposable agent-run storage.
//!
//! Database history retention (`agent_run_store::retention`) expires durable
//! runs fourteen days after they finish and keeps only the newest bounded set
//! of logical sessions. This module reclaims the files those runs leave
//! behind, plus a fixed set of other agent-run-owned disposable artifacts. It
//! never sweeps by modification time alone: every candidate must be a
//! recognized, named, currently effective-user-owned shape inside the
//! configured home, and run trees additionally require the store to prove no
//! retained row still owns or references them. Unknown names, foreign
//! ownership, unreadable metadata and unrecognized categories are always
//! retained (fail closed). Permanent configuration, accounts, credentials,
//! skills, plugins, tools, installed releases and probes are not expiring user
//! data and are never candidates.

use crate::{Result, error::invalid, fs, logging, state::Store};
use agent_run_config::{config::Config, provider_config::ProviderConfig};
use agent_run_domain::domain::AgentId;
use agent_run_store::retention::StorageProtection;
use fs2::FileExt;
use std::{
    collections::{BTreeMap, BTreeSet},
    ffi::OsString,
    fs::OpenOptions,
    os::unix::fs::OpenOptionsExt,
    os::{
        fd::{AsRawFd, FromRawFd},
        unix::ffi::OsStrExt,
    },
    path::{Path, PathBuf},
    sync::{Mutex, OnceLock},
    time::{Duration, Instant},
};

/// Disposable storage expires with its durable history: fourteen days.
pub const STORAGE_SECONDS: f64 = agent_run_store::retention::HISTORY_SECONDS;
/// Operational component and daemon log files are kept for thirty days.
///
/// This is deliberately longer than [`STORAGE_SECONDS`]: a per-run model
/// transcript or journal is execution history and follows the fourteen-day
/// rule with its run directories, while component/daemon logs describe the
/// service itself and stay useful for a longer operational window.
pub const LOG_SECONDS: f64 = 30.0 * 24.0 * 3600.0;
/// Maximum tree roots one pass may begin removing.
const TREE_ROOTS_PER_PASS: usize = 16;
/// Maximum descendant unlinks one pass performs, in addition to the bounded tree roots.
const ENTRY_UNLINKS_PER_PASS: usize = 256;
/// Maximum stale-socket probes one pass performs.
const SOCKET_PROBES_PER_PASS: usize = 16;
/// Maximum log/config backup files one pass removes.
const FILE_REMOVALS_PER_PASS: usize = 64;
/// Maximum remembered namespaces in one unfinished scan round.
const ROUND_KEYS_LIMIT: usize = 20_000;
/// Component labels this codebase uses for direct UTC-daily files.
const LOG_COMPONENTS: [&str; 6] = ["cli", "api", "api-serve", "mcp", "services", "supervisor"];

/// The effective user id; retention only ever touches its own entries.
fn euid() -> u32 {
    // SAFETY: geteuid inspects process identity and takes no pointers.
    unsafe { libc::geteuid() }
}

/// Creation time encoded in one canonical run id (`ag-YYYYMMDD-HHMMSS-hex`).
///
/// Returns the UTC creation instant in Unix seconds, or `None` when the name
/// is not a canonical id with a sane timestamp. The id is immutable, so this
/// stays correct across partial removals and restarts, unlike directory
/// modification times which any unlink refreshes.
fn run_id_created(id: &str) -> Option<f64> {
    id.parse::<AgentId>().ok()?;
    let year: i32 = id[3..7].parse().ok()?;
    if !(2020..=2100).contains(&year) {
        return None;
    }
    let date = chrono::NaiveDate::parse_from_str(&id[3..11], "%Y%m%d").ok()?;
    let hour: u32 = id[12..14].parse().ok()?;
    let minute: u32 = id[14..16].parse().ok()?;
    let second: u32 = id[16..18].parse().ok()?;
    Some(
        date.and_hms_opt(hour, minute, second)?
            .and_utc()
            .timestamp() as f64,
    )
}

/// Creation time encoded in an all-digit timestamp directory name.
///
/// Accepts second, millisecond, microsecond and nanosecond stamps by dividing
/// until the value is plausibly seconds; returns `None` for other shapes.
fn stamp_created(name: &str) -> Option<f64> {
    if !(9..=19).contains(&name.len()) || !name.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let mut value: u64 = name.parse().ok()?;
    while value > 100_000_000_000 {
        value /= 1000;
    }
    if !(1_000_000_000..100_000_000_000).contains(&value) {
        return None;
    }
    Some(value as f64)
}

/// Creation time encoded in one migration snapshot name (`<seconds>-<hex>-<kind>`).
fn migration_created(name: &str) -> Option<f64> {
    let (seconds, rest) = name.split_once('-')?;
    let (uuid, kind) = rest.split_once('-')?;
    if uuid.len() != 32
        || !uuid.bytes().all(|byte| byte.is_ascii_hexdigit())
        || !matches!(kind, "v1-to-v2" | "v2-state-upgrade")
    {
        return None;
    }
    if seconds.len() < 9 || seconds.len() > 11 {
        return None;
    }
    stamp_created(seconds)
}

/// One bounded sweep's remaining budgets and removed-entry counter.
struct Pass {
    /// Remaining directory roots that may begin draining.
    roots: usize,
    /// Remaining file and nested-directory unlinks inside approved trees.
    unlinks: usize,
    /// Remaining nonblocking socket probes.
    sockets: usize,
    /// Remaining standalone file unlinks.
    files: usize,
    /// Number of entries actually removed this pass.
    removed: usize,
    /// Remaining directory entries that may be inspected this pass.
    scans: usize,
    /// A live scan has more entries, so broker maintenance should retry soon.
    pending: bool,
    /// A directory read or safety limit failed; the pass cannot claim success.
    failed: bool,
    /// Home path and inode prefix that prevents cross-home cursor reuse.
    home_key: String,
    /// Two-second cooperative deadline for this filesystem pass.
    deadline: Instant,
}

/// One retained live scan, rejected when its directory's inode changes.
struct LiveScan {
    /// Directory identity captured before opening the stream.
    identity: (u64, u64),
    /// Exclusively owned live directory offset.
    scan: fs::DirScan,
    /// Last access used to evict the oldest stream at the descriptor ceiling.
    last_used: Instant,
}

/// Live offsets and completed namespaces for one in-memory scan round.
#[derive(Default)]
struct ScanRegistry {
    /// Open streams still traversing a directory.
    active: BTreeMap<String, LiveScan>,
    /// Namespaces whose EOF was seen this round; later passes may revisit them
    /// to reach child scans without making their size hold the round open.
    done: BTreeSet<String>,
    /// Namespaces skipped or still scanning because a pass exhausted its budget.
    needed: BTreeSet<String>,
}

/// Broker-local scan rounds only; restart begins at the start of each directory.
static SCANS: OnceLock<Mutex<ScanRegistry>> = OnceLock::new();

/// Returns the shared bounded scan registry without opening directories.
fn scans() -> &'static Mutex<ScanRegistry> {
    SCANS.get_or_init(|| Mutex::new(ScanRegistry::default()))
}

impl Pass {
    /// Starts a pass with finite work budgets and a home-scoped live-scan key.
    fn new(home: &Path, root: &fs::Dir) -> Result<Self> {
        let identity = root.entry(None)?;
        Ok(Self {
            roots: TREE_ROOTS_PER_PASS,
            unlinks: ENTRY_UNLINKS_PER_PASS,
            sockets: SOCKET_PROBES_PER_PASS,
            files: FILE_REMOVALS_PER_PASS,
            removed: 0,
            scans: 1024,
            pending: false,
            failed: false,
            home_key: format!("{}:{}:{}", home.display(), identity.device, identity.inode),
            deadline: Instant::now() + Duration::from_secs(2),
        })
    }

    /// Scans one finite batch, advancing a live DIR even when entries are retained.
    fn list(&mut self, dir: &fs::Dir, key: &str) -> Option<Vec<OsString>> {
        let full_key = format!("{}:{key}", self.home_key);
        if Instant::now() >= self.deadline {
            self.pending = true;
            scans()
                .lock()
                .expect("scan lock is not poisoned")
                .needed
                .insert(full_key);
            return None;
        }
        if self.scans == 0 {
            self.pending = true;
            scans()
                .lock()
                .expect("scan lock is not poisoned")
                .needed
                .insert(full_key);
            return None;
        }
        // Match the tree-root budget so an undeletable first batch cannot
        // consume every removal slot and reset the scan cursor at EOF.
        let limit = self.scans.min(TREE_ROOTS_PER_PASS);
        let identity = match dir.entry(None) {
            Ok(entry) => (entry.device, entry.inode),
            Err(_) => {
                self.failed = true;
                return None;
            }
        };
        let mut registry = scans().lock().expect("scan lock is not poisoned");
        if registry
            .active
            .get(&full_key)
            .is_some_and(|scan| scan.identity != identity)
        {
            registry.active.remove(&full_key);
            registry.done.remove(&full_key);
            registry.needed.insert(full_key.clone());
        }
        let completed_before = registry.done.contains(&full_key);
        if !registry.active.contains_key(&full_key) {
            if registry.active.len() >= 64
                && let Some(oldest) = registry
                    .active
                    .iter()
                    .min_by_key(|(_, scan)| scan.last_used)
                    .map(|(name, _)| name.clone())
            {
                registry.active.remove(&oldest);
                // A vanished directory must not leave an orphaned needed
                // key holding the round open forever. Its parent will be
                // rechecked in the next round if the path still exists.
                if !registry.done.contains(&oldest) {
                    registry.needed.remove(&oldest);
                    self.pending = true;
                }
            }
            let scan = match dir.scan() {
                Ok(scan) => scan,
                Err(_) => {
                    self.failed = true;
                    return None;
                }
            };
            registry.active.insert(
                full_key.clone(),
                LiveScan {
                    identity,
                    scan,
                    last_used: Instant::now(),
                },
            );
        }
        let current = registry.active.get_mut(&full_key).expect("inserted scan");
        current.last_used = Instant::now();
        let result = current.scan.next_batch(limit);
        match result {
            Ok((names, done)) => {
                self.scans -= names.len().max(1);
                if done {
                    registry.active.remove(&full_key);
                    registry.done.insert(full_key.clone());
                    registry.needed.remove(&full_key);
                } else if !completed_before {
                    registry.needed.insert(full_key.clone());
                    self.pending = true;
                }
                Some(names)
            }
            Err(_) => {
                self.failed = true;
                None
            }
        }
    }

    /// Reports whether this directory still has entries in its live scan.
    fn pending_for(&self, key: &str) -> bool {
        scans()
            .lock()
            .expect("scan lock is not poisoned")
            .active
            .contains_key(&format!("{}:{key}", self.home_key))
    }

    /// Forgets a completed tree after its verified directory inode was removed.
    fn forget_tree(&self, device: u64, inode: u64) {
        let key = format!("{}:tree:{device}:{inode}", self.home_key);
        let mut registry = scans().lock().expect("scan lock is not poisoned");
        registry.active.remove(&key);
        registry.done.remove(&key);
        registry.needed.remove(&key);
    }

    /// Completes the round once every discovered namespace reached EOF.
    fn finish(&self) -> Result<bool> {
        let prefix = format!("{}:", self.home_key);
        let mut registry = scans().lock().expect("scan lock is not poisoned");
        if registry.done.len() + registry.needed.len() > ROUND_KEYS_LIMIT {
            registry.active.retain(|key, _| !key.starts_with(&prefix));
            registry.done.retain(|key| !key.starts_with(&prefix));
            registry.needed.retain(|key| !key.starts_with(&prefix));
            return Err(invalid("filesystem retention round exceeds bound"));
        }
        if self.failed {
            return Err(invalid("filesystem retention scan incomplete"));
        }
        let unfinished = self.pending
            || registry.needed.iter().any(|key| key.starts_with(&prefix))
            || registry
                .active
                .keys()
                .any(|key| key.starts_with(&prefix) && !registry.done.contains(key));
        if !unfinished {
            registry.active.retain(|key, _| !key.starts_with(&prefix));
            registry.done.retain(|key| !key.starts_with(&prefix));
            registry.needed.retain(|key| !key.starts_with(&prefix));
        }
        Ok(unfinished)
    }
}

/// Runs one bounded filesystem retention pass for `home` at Unix time `now`.
///
/// Returns the removed-entry count, or one when traversal needs another pass
/// without having removed an entry. Zero means the pass found no actionable work. Each
/// pass removes at most a fixed number of tree roots and unlinks, so a huge
/// tree drains over successive passes instead of blocking one call; partially
/// removed trees stay eligible because eligibility comes from immutable names
/// and durable markers, never from timestamps the removal itself refreshes.
/// The store is only read with short bounded queries between filesystem work,
/// never inside a write transaction. The same pass runs one bounded
/// reference-aware collection over the shared managed-asset store, under its
/// own nonblocking publish lock, so a busy publisher never delays the rest of
/// retention. Callers should rerun after one second while work remains and
/// hourly when idle; database expiry keeps its own independent schedule.
pub fn sweep(home: &Path, now: f64, store: &mut Store) -> Result<usize> {
    if !now.is_finite() || now < LOG_SECONDS {
        return Err(invalid("filesystem retention requires a finite Unix time"));
    }
    let root = fs::Dir::open(home)?;
    let identity = root.entry(None)?;
    if identity.kind != fs::EntryType::Directory || identity.uid != euid() {
        return Err(invalid(
            "filesystem retention requires an owned home directory",
        ));
    }
    let mut proof = store.storage_protection_snapshot()?;
    let config = current_config(home, &root)
        .ok_or_else(|| invalid("current configuration cannot prove filesystem retention safety"))?;
    collect_config_paths(&config, &mut proof);
    let mut pass = Pass::new(home, &root)?;
    run_directories(home, &root, now, &proof, store, &mut pass);
    standalone_backups(home, &root, now, &proof, &mut pass);
    migration_snapshots(home, &root, now, &proof, &mut pass);
    config_profile_backups(home, &root, now, &config, &proof, &mut pass);
    aged_logs(home, &root, now, &proof, &mut pass);
    stale_sockets(home, &root, &proof, &mut pass);
    // Shared-store collection reports its own backlog and never fails the
    // retention pass: a busy publish lock or incomplete evidence simply
    // retries on the next cycle, while everything above still makes progress.
    let collected = crate::storage_gc::sweep(store, home, crate::storage_gc::Mode::Apply)
        .map(|outcome| (outcome.removed(), outcome.backlog()))
        .unwrap_or((0, true));
    pass.removed += collected.0;
    if pass.removed > 0
        && let Some(logger) = logging::configured()
    {
        logger.log(
            logging::Level::Debug,
            &format!("housekeeping removed={}", pass.removed),
        );
    }
    let more = pass.finish()?;
    Ok(pass.removed + usize::from((more || collected.1) && pass.removed == 0))
}

/// True when one canonical run id's encoded creation is older than the cutoff.
fn older_than(created: f64, now: f64, window: f64) -> bool {
    created.is_finite() && now - created > window
}

/// Reads and parses the current configuration through the no-follow home descriptor.
/// Absence, invalid TOML, or an oversized file gives no deletion proof.
fn current_config(home: &Path, root: &fs::Dir) -> Option<toml::Value> {
    let bytes = root
        .optional(Path::new("config.toml"), 1024 * 1024)
        .ok()??;
    let text = std::str::from_utf8(&bytes).ok()?;
    let raw: toml::Value = toml::from_str(text).ok()?;
    if ProviderConfig::parse(text, home).is_err() {
        // Match the schema-1 loader's retired runtime handling and validate
        // the same bytes before they can authorize filesystem deletion.
        let mut parsed = raw.clone();
        if let Some(runtimes) = parsed
            .get_mut("runtimes")
            .and_then(toml::Value::as_table_mut)
        {
            runtimes.remove("opencode");
        }
        let rewritten = toml::to_string(&parsed).ok()?;
        let mut legacy: Config = toml::from_str(&rewritten).ok()?;
        legacy.validate(home).ok()?;
    }
    Some(raw)
}

/// Adds absolute paths and file credential references from the parsed live configuration.
fn collect_config_paths(value: &toml::Value, proof: &mut StorageProtection) {
    match value {
        toml::Value::String(text) => proof.protect_path(text),
        toml::Value::Array(values) => {
            for value in values {
                collect_config_paths(value, proof);
            }
        }
        toml::Value::Table(values) => {
            for value in values.values() {
                collect_config_paths(value, proof);
            }
        }
        _ => {}
    }
}

/// Returns every absolute path the current configuration names.
///
/// Shared-store collection shares this evidence with retention: a service
/// command, working directory or credential file may point directly into a
/// shared tree or payload, so the live configuration pins those objects
/// exactly as the frozen identities and service definitions do. A home with
/// no configuration file has no configuration references, which is proof; a
/// configuration that exists but cannot be read or parsed is **not** proof of
/// an empty set — it is uncertainty, returned as an error so the caller
/// retains everything and retries.
pub(crate) fn config_paths(home: &Path) -> Result<Vec<PathBuf>> {
    let root = fs::Dir::open(home)?;
    let bytes = root
        .optional(Path::new("config.toml"), 1024 * 1024)?
        .ok_or_else(|| invalid("current configuration cannot prove store safety"))?;
    let text = std::str::from_utf8(&bytes)
        .map_err(|_| invalid("current configuration is not valid UTF-8"))?;
    let _raw: toml::Value = toml::from_str(text)
        .map_err(|_| invalid("current configuration cannot prove store safety"))?;
    let config = current_config(home, &root)
        .ok_or_else(|| invalid("current configuration cannot prove store safety"))?;
    let mut proof = agent_run_store::retention::StorageProtection::empty();
    collect_config_paths(&config, &mut proof);
    Ok(proof.protected_paths().map(Path::to_path_buf).collect())
}

/// Reports an exact live configuration string or path-basename reference.
fn config_references(value: &toml::Value, name: &str) -> bool {
    match value {
        toml::Value::String(text) => {
            text == name
                || Path::new(text.strip_prefix("file:").unwrap_or(text))
                    .file_name()
                    .is_some_and(|part| part == name)
        }
        toml::Value::Array(values) => values.iter().any(|value| config_references(value, name)),
        toml::Value::Table(values) => values.values().any(|value| config_references(value, name)),
        _ => false,
    }
}

/// True when the pass-wide count boundary proves a tree's lineage ranks
/// outside the protected set and a fresh indexed read finds no admitted row
/// owning it.
///
/// `boundary` is the snapshot's newest-hundredth session boundary (`None`
/// below the cap proves nothing), `created` is the tree's encoded creation,
/// and the row check runs only after the tree was observed on disk, so an
/// admission landing after the snapshot still wins. Any store error counts
/// as "not retired": the tree stays and the next pass retries.
fn count_retired(store: &Store, boundary: Option<f64>, id: &str, created: f64) -> bool {
    boundary.is_some_and(|boundary| created < boundary)
        && matches!(store.run_admitted(id), Ok(false))
}

/// Removes orphan `agents/<id>` trees and pruned runtime run directories.
///
/// A tree qualifies only when its canonical id encodes a creation older than
/// fourteen days, or when the pass can prove it count-retired: the snapshot's
/// count boundary exists (see [`StorageProtection::count_boundary`]), the id
/// predates it, and no agents row for the id exists as of a fresh indexed
/// read taken after this pass observed the tree (see [`Store::run_admitted`]).
/// That fresh row check is what makes the decision admission-safe — a run
/// admitted in the same wall-clock second as the pass, an id generated before
/// its admission, or a rolled-back clock all still resolve through the
/// durable row rather than the id's second-resolution timestamp — and the
/// pass-wide boundary is what keeps the per-tree check to one primary-key
/// lookup instead of a ranking scan per candidate. Together they let
/// count-expired trees of any age reclaim, at or after the database has
/// converged to exactly the newest hundred sessions, instead of waiting out
/// the fourteen-day orphan window. Either way the protection snapshot must
/// also prove no retained row owns the id or references the exact path
/// (covering resumed children and identities that embed `runtime_home`).
/// A tree whose recency the store cannot prove keeps the age rule. Read
/// failures fail closed and retain the tree.
fn run_directories(
    home: &Path,
    root: &fs::Dir,
    now: f64,
    proof: &StorageProtection,
    store: &Store,
    pass: &mut Pass,
) {
    let boundary = proof.count_boundary();
    let Some(agents) = open_owned(root, "agents") else {
        return;
    };
    let Some(names) = pass.list(&agents, "agents") else {
        return;
    };
    for name in names {
        if pass.roots == 0 {
            return;
        }
        let Some(id) = name.to_str() else { continue };
        let Some(created) = run_id_created(id) else {
            continue;
        };
        if !older_than(created, now, STORAGE_SECONDS)
            && !count_retired(store, boundary, id, created)
        {
            continue;
        }
        if proof.retains(id, &home.join("agents").join(id)) {
            continue;
        }
        remove_root(&agents, id, false, pass);
    }
    let Some(runtimes) = open_owned(root, "runtimes") else {
        return;
    };
    let Some(names) = pass.list(&runtimes, "runtimes") else {
        return;
    };
    for runtime in names {
        let Some(runtime) = runtime.to_str() else {
            continue;
        };
        let Some(runtime_dir) = open_owned(&runtimes, runtime) else {
            continue;
        };
        let Some(bases) = pass.list(&runtime_dir, &format!("runtime:{runtime}")) else {
            continue;
        };
        for base in bases {
            let Some(base) = base.to_str() else { continue };
            if !is_runtime_base(base) {
                continue;
            }
            let Some(base_dir) = open_owned(&runtime_dir, base) else {
                continue;
            };
            let Some(runs) = open_owned(&base_dir, "runs") else {
                continue;
            };
            let Some(names) = pass.list(&runs, &format!("runs:{runtime}/{base}")) else {
                continue;
            };
            for name in names {
                if pass.roots == 0 {
                    return;
                }
                let Some(id) = name.to_str() else { continue };
                let Some(created) = run_id_created(id) else {
                    continue;
                };
                if !older_than(created, now, STORAGE_SECONDS)
                    && !count_retired(store, boundary, id, created)
                {
                    continue;
                }
                if proof.retains(
                    id,
                    &home
                        .join("runtimes")
                        .join(runtime)
                        .join(base)
                        .join("runs")
                        .join(id),
                ) {
                    continue;
                }
                remove_root(&runs, id, false, pass);
            }
        }
    }
}

/// Recognizes runtime home bases: `home` and labelled `home@<account>` forms.
fn is_runtime_base(name: &str) -> bool {
    match name.strip_prefix("home") {
        None => false,
        Some("") => true,
        Some(rest) => match rest.strip_prefix('@') {
            Some(label) => !label.is_empty() && !label.contains('@'),
            None => false,
        },
    }
}

/// Expires timestamped `standalone/backups` entries after fourteen days.
///
/// The persistent installer lock is acquired nonblocking and held for the
/// pass. A missing, corrupt, pending or unknown deployment journal blocks
/// deletion; completed backups may expire, including the latest completed
/// backup. Installed releases and `current` are never touched.
fn standalone_backups(
    home: &Path,
    root: &fs::Dir,
    now: f64,
    proof: &StorageProtection,
    pass: &mut Pass,
) {
    let Some(standalone) = open_owned(root, "standalone") else {
        return;
    };
    // The install lock persists after installation: hold the same advisory lock
    // while judging backups instead of treating its mere presence as activity.
    let lock = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(home.join("standalone/.install.lock"));
    let Ok(lock) = lock else { return };
    if lock.try_lock_exclusive().is_err() {
        return;
    }
    let deploy_lock = match standalone.entry(Some(Path::new("deploy.lock"))) {
        Ok(_) => {
            let Ok(file) = OpenOptions::new()
                .read(true)
                .write(true)
                .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
                .open(home.join("standalone/deploy.lock"))
            else {
                return;
            };
            if file.try_lock_exclusive().is_err() {
                return;
            }
            Some(file)
        }
        Err(crate::Error::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(_) => return,
    };
    let _guards = (lock, deploy_lock);
    match standalone.optional(Path::new("deploy.json"), 8192) {
        Ok(None) => {}
        Ok(Some(bytes)) => {
            let Ok(journal) = serde_json::from_slice::<serde_json::Value>(&bytes) else {
                return;
            };
            let Some(phase) = journal.get("phase").and_then(serde_json::Value::as_str) else {
                return;
            };
            if !matches!(
                phase,
                "committed" | "recovered" | "rolled_forward" | "rolled_back"
            ) {
                return;
            }
        }
        Err(_) => return,
    }
    let Some(backups) = open_owned(&standalone, "backups") else {
        return;
    };
    let Some(names) = pass.list(&backups, "standalone/backups") else {
        return;
    };
    for name in names {
        if pass.roots == 0 {
            return;
        }
        let Some(name) = name.to_str() else { continue };
        let Some(created) = stamp_created(name) else {
            continue;
        };
        if !older_than(created, now, STORAGE_SECONDS) {
            continue;
        }
        if proof.retains("", &home.join("standalone/backups").join(name)) {
            continue;
        }
        remove_root(&backups, name, false, pass);
    }
}

/// Expires completed migration snapshots and their applied markers.
///
/// Eligibility requires the sibling `<snapshot>.applied.json` marker; while
/// `migrations/in-progress.json` exists nothing in the category is touched.
/// A snapshot directory must carry `COMPLETE`, or be empty after a prior
/// COMPLETE-last partial drain. Snapshots are
/// owner-read-only (`0500`/`0400`); write permission is restored only on the
/// descriptor of the already-judged-disposable directory. If a crash removes
/// the snapshot but not its marker, a later pass unlinks just the marker, so
/// partial passes always resume safely.
fn migration_snapshots(
    home: &Path,
    root: &fs::Dir,
    now: f64,
    proof: &StorageProtection,
    pass: &mut Pass,
) {
    let Some(migrations) = open_owned(root, "migrations") else {
        return;
    };
    if migrations
        .entry(Some(Path::new("in-progress.json")))
        .is_ok()
    {
        return;
    }
    let Some(names) = pass.list(&migrations, "migrations") else {
        return;
    };
    for name in names {
        let Some(name) = name.to_str() else { continue };
        let Some(marker) = name.strip_suffix(".applied.json") else {
            continue;
        };
        if pass.files == 0 {
            return;
        }
        let Some(created) = migration_created(marker) else {
            continue;
        };
        if !older_than(created, now, STORAGE_SECONDS) {
            continue;
        }
        if proof.retains("", &home.join("migrations").join(marker))
            || proof.retains("", &home.join("migrations").join(name))
        {
            continue;
        }
        let snapshot = PathBuf::from(marker);
        match migrations.entry(Some(&snapshot)) {
            Ok(entry) => {
                let empty_after_partial = migrations
                    .subdir(&snapshot)
                    .ok()
                    .and_then(|dir| dir.list_batch(1).ok())
                    .is_some_and(|(names, done)| done && names.is_empty());
                if entry.kind == fs::EntryType::Directory
                    && entry.uid == euid()
                    && (empty_after_partial
                        || migrations
                            .entry(Some(&snapshot.join("COMPLETE")))
                            .is_ok_and(|complete| {
                                complete.kind == fs::EntryType::File && complete.uid == euid()
                            }))
                {
                    remove_root(&migrations, marker, true, pass);
                }
            }
            // The snapshot is already gone; finish by removing its marker.
            Err(crate::Error::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
                remove_owned_file(&migrations, name, now, STORAGE_SECONDS, pass);
            }
            // Unknown snapshot state fails closed and retains the marker.
            Err(_) => {}
        }
    }
}

/// Expires recognized obsolete configuration and profile backups after fourteen days.
///
/// Recognized shapes are `config.toml.bak[.<suffix>]`, `config.toml.orig`,
/// `config.toml.target-<suffix>` at the home root and `*.md.bak[.<suffix>]`
/// inside `profiles`/`profiles-v2`. A candidate whose name the current
/// `config.toml` still references is retained, protecting active
/// configuration and account credential wiring; live configuration,
/// profiles and account data never match these shapes.
fn config_profile_backups(
    home: &Path,
    root: &fs::Dir,
    now: f64,
    config: &toml::Value,
    proof: &StorageProtection,
    pass: &mut Pass,
) {
    let Some(names) = pass.list(root, "config-home") else {
        return;
    };
    for name in names {
        if pass.files == 0 {
            return;
        }
        let Some(name) = name.to_str() else { continue };
        if !is_config_backup(name)
            || config_references(config, name)
            || proof.retains("", &home.join(name))
        {
            continue;
        }
        remove_owned_file(root, name, now, STORAGE_SECONDS, pass);
    }
    for profiles in ["profiles", "profiles-v2"] {
        let Some(dir) = open_owned(root, profiles) else {
            continue;
        };
        let Some(names) = pass.list(&dir, &format!("profiles:{profiles}")) else {
            continue;
        };
        for name in names {
            if pass.files == 0 {
                return;
            }
            let Some(name) = name.to_str() else { continue };
            if !is_profile_backup(name)
                || config_references(config, name)
                || proof.retains("", &home.join(profiles).join(name))
            {
                continue;
            }
            remove_owned_file(&dir, name, now, STORAGE_SECONDS, pass);
        }
    }
}

/// Recognizes profile backup names: `<profile>.md.bak[.<suffix>]`.
fn is_profile_backup(name: &str) -> bool {
    match name.find(".md.bak") {
        Some(at) => {
            let suffix = &name[at + ".md.bak".len()..];
            suffix.is_empty()
                || ((suffix.starts_with('.') || suffix.starts_with('-'))
                    && suffix.len() > 1
                    && suffix[1..]
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-'))
        }
        None => false,
    }
}

/// Recognizes home-root configuration backup names this codebase creates.
fn is_config_backup(name: &str) -> bool {
    (name == "config.toml.bak"
        || name.starts_with("config.toml.bak.")
        || name.starts_with("config.toml.bak-"))
        || name == "config.toml.orig"
        || name.starts_with("config.toml.target-")
}

/// Expires aged operational log files after thirty days.
///
/// Only direct UTC-daily files are eligible. The logger reopens the current
/// day's file before its next write, so an old dated inode cannot receive a
/// future line. Undated legacy component and launchd stdout/stderr files are
/// retained: age or an advisory lock cannot prove a writer closed them.
fn aged_logs(home: &Path, root: &fs::Dir, now: f64, proof: &StorageProtection, pass: &mut Pass) {
    let Some(logs) = open_owned(root, "logs") else {
        return;
    };
    let Some(names) = pass.list(&logs, "logs") else {
        return;
    };
    for name in names {
        if pass.files == 0 {
            return;
        }
        let Some(name) = name.to_str() else { continue };
        let Some(created) = daily_log_created(name) else {
            continue;
        };
        if !older_than(created, now, LOG_SECONDS) {
            continue;
        }
        if proof.retains("", &home.join("logs").join(name)) {
            continue;
        }
        remove_owned_file(&logs, name, now, LOG_SECONDS, pass);
    }
}

/// Parses one recognized daily component filename into its UTC midnight timestamp.
fn daily_log_created(name: &str) -> Option<f64> {
    let stem = name.strip_suffix(".log")?;
    let (component, date) = stem.rsplit_once('.')?;
    if !LOG_COMPONENTS.contains(&component) || !date.is_ascii() {
        return None;
    }
    let day = chrono::NaiveDate::parse_from_str(date, "%Y-%m-%d").ok()?;
    Some(day.and_hms_opt(0, 0, 0)?.and_utc().timestamp() as f64)
}

/// Reclaims stale Desktop relay sockets at the home root.
///
/// A socket is stale only when a fresh nonblocking connect is refused and the inode is
/// unchanged across the probe; live sockets and unknown outcomes are retained.
/// No process identity is guessed and nothing is killed.
fn stale_sockets(home: &Path, root: &fs::Dir, proof: &StorageProtection, pass: &mut Pass) {
    let Some(names) = pass.list(root, "sockets") else {
        return;
    };
    for name in names {
        if pass.sockets == 0 {
            return;
        }
        let Some(name) = name.to_str() else { continue };
        if !is_relay_socket(name) {
            continue;
        }
        if proof.retains("", &home.join(name)) {
            continue;
        }
        let rel = Path::new(name);
        let Ok(entry) = root.entry(Some(rel)) else {
            continue;
        };
        if entry.kind != fs::EntryType::Special || !entry.socket || entry.uid != euid() {
            continue;
        }
        pass.sockets -= 1;
        // A full listen backlog returns pending/uncertain immediately; only an
        // explicit refused or vanished endpoint proves a socket stale.
        if !matches!(probe_socket(&home.join(name)), Ok(false)) {
            continue;
        }
        // Identity must still be the probed object immediately before unlink.
        match root.entry(Some(rel)) {
            Ok(current)
                if current.device == entry.device
                    && current.inode == entry.inode
                    && current.socket =>
            {
                if root.remove(rel).is_ok() {
                    pass.removed += 1;
                }
            }
            _ => continue,
        }
    }
}

/// Probes a Unix socket without ever waiting for a full listen backlog.
/// Returns `false` only for a definite refused or vanished endpoint.
fn probe_socket(path: &Path) -> std::io::Result<bool> {
    let bytes = path.as_os_str().as_bytes();
    // SAFETY: sockaddr_un is a plain C buffer; address fields are filled before connect.
    let mut address = unsafe { std::mem::zeroed::<libc::sockaddr_un>() };
    if bytes.len() >= address.sun_path.len() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "socket path too long",
        ));
    }
    let length = std::mem::offset_of!(libc::sockaddr_un, sun_path) + bytes.len() + 1;
    address.sun_family = libc::AF_UNIX as libc::sa_family_t;
    #[cfg(target_os = "macos")]
    {
        address.sun_len = length as u8;
    }
    for (target, source) in address.sun_path.iter_mut().zip(bytes) {
        *target = *source as libc::c_char;
    }
    // SAFETY: socket returns one owned descriptor; File closes it on every path.
    let descriptor = unsafe { libc::socket(libc::AF_UNIX, libc::SOCK_STREAM, 0) };
    if descriptor < 0 {
        return Err(std::io::Error::last_os_error());
    }
    // SAFETY: socket returned a valid descriptor which is uniquely owned here.
    let socket = unsafe { std::fs::File::from_raw_fd(descriptor) };
    // SAFETY: fcntl modifies only this newly created descriptor.
    if unsafe { libc::fcntl(socket.as_raw_fd(), libc::F_SETFL, libc::O_NONBLOCK) } < 0 {
        return Err(std::io::Error::last_os_error());
    }
    // SAFETY: address is initialized and length includes the terminating NUL.
    if unsafe {
        libc::connect(
            socket.as_raw_fd(),
            (&address as *const libc::sockaddr_un).cast(),
            length as libc::socklen_t,
        )
    } == 0
    {
        return Ok(true);
    }
    let error = std::io::Error::last_os_error();
    match error.raw_os_error() {
        Some(libc::ECONNREFUSED) | Some(libc::ENOENT) => Ok(false),
        _ => Err(error),
    }
}

/// Recognizes Desktop relay socket names (`ar-cdx-v<version>-...sock`).
fn is_relay_socket(name: &str) -> bool {
    let rest = match name.strip_prefix("ar-cdx-v") {
        Some(rest) => rest,
        None => return false,
    };
    name.ends_with(".sock")
        && rest.starts_with(|b: char| b.is_ascii_digit())
        && name.len() > "ar-cdx-v1-.sock".len()
}

/// Opens one direct child of `root` when it is an effective-user-owned directory.
fn open_owned(root: &fs::Dir, name: &str) -> Option<fs::Dir> {
    let rel = Path::new(name);
    let entry = root.entry(Some(rel)).ok()?;
    if entry.kind != fs::EntryType::Directory || entry.uid != euid() {
        return None;
    }
    let dir = root.subdir(rel).ok()?;
    match dir.entry(None) {
        Ok(current) if current.device == entry.device && current.inode == entry.inode => Some(dir),
        _ => None,
    }
}

/// Removes one recognized owned regular file older than `window` by last write.
fn remove_owned_file(dir: &fs::Dir, name: &str, now: f64, window: f64, pass: &mut Pass) {
    let rel = Path::new(name);
    let Ok(entry) = dir.entry(Some(rel)) else {
        return;
    };
    if entry.kind != fs::EntryType::File || entry.uid != euid() || entry.modified > now - window {
        return;
    }
    match dir.entry(Some(rel)) {
        Ok(current)
            if current.device == entry.device
                && current.inode == entry.inode
                && dir.remove(rel).is_ok() =>
        {
            pass.files -= 1;
            pass.removed += 1;
        }
        _ => {}
    }
}

/// Removes one judged-disposable directory tree through its parent, bounded by the pass.
///
/// The root's identity is re-verified on the opened descriptor; every entry is
/// then unlinked through no-follow parent descriptors with a fresh ownership
/// check, recursing only into owned real directories. Special entries (FIFOs,
/// devices, sockets) inside a tree are left in place, so a tree holding one
/// stays recognizable but harmless. When the unlink budget runs out the tree
/// remains partially removed and eligible again on the next pass, because
/// eligibility never depends on timestamps this removal changes.
fn remove_root(parent: &fs::Dir, name: &str, preserve_complete: bool, pass: &mut Pass) {
    if pass.roots == 0 || pass.unlinks == 0 {
        return;
    }
    let rel = Path::new(name);
    let Ok(entry) = parent.entry(Some(rel)) else {
        return;
    };
    if entry.kind != fs::EntryType::Directory || entry.uid != euid() {
        return;
    }
    let Ok(dir) = parent.subdir(rel) else { return };
    match dir.entry(None) {
        Ok(current) if current.device == entry.device && current.inode == entry.inode => {}
        _ => return,
    }
    pass.roots -= 1;
    // Owner-read-only snapshot directories need write permission to unlink
    // their contents; this fchmod can only ever affect this owned descriptor.
    if dir.permit_owner_write().is_err() {
        return;
    }
    if !drain(&dir, preserve_complete, 0, pass) {
        return;
    }
    match parent.entry(Some(rel)) {
        Ok(current)
            if current.device == entry.device
                && current.inode == entry.inode
                && current.kind == fs::EntryType::Directory
                && parent.remove_directory(rel).is_ok() =>
        {
            pass.removed += 1;
            pass.forget_tree(entry.device, entry.inode);
        }
        _ => {}
    }
}

/// Unlinks every entry of `dir` under the pass budget; true when it became empty.
fn drain(dir: &fs::Dir, preserve_complete: bool, depth: usize, pass: &mut Pass) -> bool {
    if depth >= 32 || Instant::now() >= pass.deadline {
        pass.failed = true;
        return false;
    }
    let Ok(identity) = dir.entry(None) else {
        return false;
    };
    let key = format!("tree:{}:{}", identity.device, identity.inode);
    let Some(names) = pass.list(dir, &key) else {
        return false;
    };
    let mut empty = true;
    for name in names {
        let rel = PathBuf::from(name);
        if preserve_complete && rel == Path::new("COMPLETE") {
            continue;
        }
        let Ok(entry) = dir.entry(Some(&rel)) else {
            return false;
        };
        if entry.uid != euid() {
            return false;
        }
        match entry.kind {
            fs::EntryType::File | fs::EntryType::Symlink => {
                if pass.unlinks == 0 {
                    return false;
                }
                match dir.entry(Some(&rel)) {
                    Ok(current)
                        if current.device == entry.device
                            && current.inode == entry.inode
                            && current.kind == entry.kind => {}
                    _ => return false,
                }
                if dir.remove(&rel).is_err() {
                    return false;
                }
                pass.unlinks -= 1;
                pass.removed += 1;
            }
            fs::EntryType::Directory => {
                if pass.unlinks == 0 {
                    return false;
                }
                let Ok(child) = dir.subdir(&rel) else {
                    return false;
                };
                match child.entry(None) {
                    Ok(current)
                        if current.device == entry.device && current.inode == entry.inode => {}
                    _ => return false,
                }
                // Nested owner-read-only directories relax the same narrow way.
                if child.permit_owner_write().is_err() {
                    return false;
                }
                if !drain(&child, false, depth + 1, pass) {
                    empty = false;
                    continue;
                }
                match dir.entry(Some(&rel)) {
                    Ok(current)
                        if current.device == entry.device
                            && current.inode == entry.inode
                            && current.kind == fs::EntryType::Directory => {}
                    _ => return false,
                }
                if pass.unlinks == 0 {
                    return false;
                }
                if dir.remove_directory(&rel).is_err() {
                    empty = false;
                } else {
                    pass.unlinks -= 1;
                    pass.removed += 1;
                    pass.forget_tree(entry.device, entry.inode);
                }
            }
            fs::EntryType::Special => empty = false,
        }
    }
    if pass.pending_for(&key) || !empty {
        return false;
    }
    if preserve_complete {
        // Earlier pages may have contained a retained special entry. A fresh
        // independent scan must prove the whole directory now contains only
        // COMPLETE before that recovery marker may be removed.
        let only_marker = dir.list_batch(2).is_ok_and(|(names, done)| {
            done && (names.is_empty() || (names.len() == 1 && names[0] == "COMPLETE"))
        });
        if !only_marker {
            return false;
        }
        let rel = Path::new("COMPLETE");
        match dir.entry(Some(rel)) {
            Ok(entry) if entry.kind == fs::EntryType::File && entry.uid == euid() => {
                if pass.unlinks == 0 {
                    return false;
                }
                match dir.entry(Some(rel)) {
                    Ok(current)
                        if current.device == entry.device && current.inode == entry.inode => {}
                    _ => return false,
                }
                if dir.remove(rel).is_err() {
                    return false;
                }
                pass.unlinks -= 1;
                pass.removed += 1;
            }
            Err(crate::Error::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {}
            _ => return false,
        }
    }
    empty
}

#[cfg(test)]
/// Retention and socket probes use private fixtures and never launch a model process.
mod tests {
    use super::*;
    use std::os::fd::AsRawFd;

    /// An admission committed after protection capture keeps its same-second
    /// tree through the real collector, even though the stale count boundary
    /// makes the name eligible and the old proof has no reference to it.
    #[test]
    fn count_collection_rechecks_admission_after_protection_capture() {
        let home = tempfile::tempdir().unwrap();
        Store::initialize(home.path()).unwrap();
        let store = Store::open(home.path()).unwrap();
        let at = 2_000_000_000.0;
        for index in 0..100 {
            let id = format!("ag-20330518-040000-{index:010x}");
            store.conn.execute(
                "INSERT INTO agents(id,runtime,model,profile,task,task_summary,workdir,
                 request_json,status,created_at,finished_at,timeout_seconds,config_revision,root_agent_id)
                 VALUES(?1,'mock','fixture','review','fixture','fixture','/tmp','{}',
                        'succeeded',?2,?2,1,'fixture',?1)",
                rusqlite::params![id, at + 1_000.0 + index as f64],
            ).unwrap();
        }
        let stale = store.storage_protection_snapshot().unwrap();
        assert_eq!(stale.count_boundary(), Some(at + 1_000.0));
        let id = "ag-20330518-033320-ffffffffff";
        let tree = home.path().join("agents").join(id);
        assert!(!stale.retains(id, &tree));
        store
            .conn
            .execute(
                "INSERT INTO agents(id,runtime,model,profile,task,task_summary,workdir,
             request_json,status,created_at,timeout_seconds,config_revision,root_agent_id)
             VALUES(?1,'mock','fixture','review','fixture','fixture','/tmp','{}',
                    'running',?2,1,'fixture',?1)",
                rusqlite::params![id, at + 0.8],
            )
            .unwrap();
        fs::private_dir(&tree).unwrap();
        std::fs::write(tree.join("live"), "fixture").unwrap();
        let root = fs::Dir::open(home.path()).unwrap();
        let mut pass = Pass::new(home.path(), &root).unwrap();
        run_directories(home.path(), &root, at + 0.9, &stale, &store, &mut pass);
        assert!(
            tree.join("live").exists(),
            "fresh admission wins over stale protection"
        );
    }

    /// A listener with zero queued slots still returns from the nonblocking probe promptly.
    #[test]
    fn zero_backlog_probe_is_bounded() {
        let home = tempfile::tempdir().unwrap();
        let path = home.path().join("ar-cdx-v4-backlog.sock");
        let listener = std::os::unix::net::UnixListener::bind(&path).unwrap();
        // SAFETY: listen changes only this test-owned listener's backlog.
        assert_eq!(unsafe { libc::listen(listener.as_raw_fd(), 0) }, 0);
        let started = Instant::now();
        let result = probe_socket(&path);
        assert!(started.elapsed() < Duration::from_millis(200));
        assert!(!matches!(result, Ok(false)), "a live listener is not stale");
    }

    /// Evicting the least recently used stream resets only that traversal and allows progress.
    #[test]
    fn scan_registry_pressure_recovers_after_eviction() {
        *scans().lock().unwrap() = ScanRegistry::default();
        let home = tempfile::tempdir().unwrap();
        for index in 0..20 {
            std::fs::write(home.path().join(format!("entry-{index:02}")), "x").unwrap();
        }
        let root = fs::Dir::open(home.path()).unwrap();
        let original = Pass::new(home.path(), &root).unwrap();
        let first_key = format!("{}:first", original.home_key);
        for index in 0..260 {
            let mut pass = Pass::new(home.path(), &root).unwrap();
            let key = if index == 0 {
                "first".to_owned()
            } else {
                format!("key-{index}")
            };
            assert_eq!(pass.list(&root, &key).unwrap().len(), 16);
        }
        assert_eq!(scans().lock().unwrap().active.len(), 64);
        assert!(!scans().lock().unwrap().active.contains_key(&first_key));
        assert!(!scans().lock().unwrap().needed.contains(&first_key));
        let mut resumed = Pass::new(home.path(), &root).unwrap();
        assert_eq!(resumed.list(&root, "first").unwrap().len(), 16);
        let mut resumed = Pass::new(home.path(), &root).unwrap();
        assert_eq!(resumed.list(&root, "first").unwrap().len(), 4);
        assert!(!resumed.pending_for("first"));
        // A failed traversal must still enforce the round's memory ceiling.
        let prefix = format!("{}:", resumed.home_key);
        scans()
            .lock()
            .unwrap()
            .needed
            .extend((0..=ROUND_KEYS_LIMIT).map(|index| format!("{prefix}excess-{index}")));
        resumed.failed = true;
        assert!(resumed.finish().is_err());
        assert!(
            !scans()
                .lock()
                .unwrap()
                .needed
                .iter()
                .any(|key| key.starts_with(&prefix))
        );
        *scans().lock().unwrap() = ScanRegistry::default();
    }
}
