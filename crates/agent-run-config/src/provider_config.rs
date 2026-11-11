//! Strict v2 provider configuration beside the still-running v1 consumer path.
//!
//! The loader owns no cache: callers retain their last valid value when
//! `load_if_changed` rejects a new file revision.

use crate::config::{
    self, Capacity, Catalog, Config, Core, Delivery, Environment, Hook, Mcp, RustRoots,
};
use agent_run_domain::{
    Result, canonical,
    catalog::{
        AccountRecord, AuthFamily, CollectorBinding, HarnessId, LimitsSource, ProviderBinding,
        ProviderCatalog, ProviderConnection, ProviderDefinition, ProviderId, ProviderModel,
    },
    error::invalid,
    types::PositiveFinite,
};
use agent_run_platform::fs;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};

/// Generic interpreters cannot identify their supplied program by executable hash.
pub fn forbidden_interpreter(path: &Path) -> bool {
    let Some(name) = path.file_name().and_then(|value| value.to_str()) else {
        return true;
    };
    let name = name.to_ascii_lowercase();
    [
        "python",
        "pypy",
        "awk",
        "gawk",
        "mawk",
        "nawk",
        "expect",
        "rscript",
        "julia",
        "erl",
        "escript",
        "ld",
        "ld.so",
        "ld-linux",
        "ld-musl",
        "node",
        "nodejs",
        "deno",
        "bun",
        "bash",
        "dash",
        "zsh",
        "ksh",
        "fish",
        "sh",
        "csh",
        "tcsh",
        "pwsh",
        "powershell",
        "perl",
        "ruby",
        "lua",
        "php",
        "tclsh",
        "wish",
        "gjs",
        "qjs",
        "jsc",
        "wasmtime",
        "wasmer",
        "env",
        "busybox",
        "java",
        "dotnet",
    ]
    .iter()
    .any(|prefix| {
        name == *prefix
            || (name.starts_with(prefix)
                && name
                    .as_bytes()
                    .get(prefix.len())
                    .is_some_and(|b| b.is_ascii_digit() || matches!(b, b'.' | b'-' | b'_')))
    })
}

/// Entries one bounded scan may visit before it fails closed.
const SCAN_ENTRY_CAP: usize = 200_000;
/// A cached scan is rebuilt after this long even when no scanned directory changed.
const SCAN_TTL: std::time::Duration = std::time::Duration::from_secs(300);

/// Installed interpreters live in executable directories; their sibling data
/// trees (site-packages, toolchain caches) can hold millions of files.
fn executable_directory(directory: &Path) -> bool {
    matches!(
        directory.file_name().and_then(|name| name.to_str()),
        Some("bin" | "sbin" | "libexec")
    )
}

/// Interpreter-named regular files and the directories that were listed for
/// them. `None` means the bounded scan was exhausted, which callers treat as a
/// refusal.
struct InterpreterScan {
    files: Vec<(PathBuf, u64)>,
    directories: Vec<PathBuf>,
}

/// A root with depth 0 lists its own files; a deeper root descends at most that
/// many directory levels and lists files only inside `bin`/`sbin`/`libexec`.
fn scan_interpreters(roots: &[(PathBuf, usize)], cap: usize) -> Option<InterpreterScan> {
    let mut scan = InterpreterScan {
        files: Vec::new(),
        directories: Vec::new(),
    };
    let mut visited = 0usize;
    let mut pending: Vec<(PathBuf, usize, usize)> = roots
        .iter()
        .map(|(path, max_depth)| (path.clone(), *max_depth, 0))
        .collect();
    while let Some((directory, max_depth, depth)) = pending.pop() {
        let listed = max_depth == 0 || executable_directory(&directory);
        let Ok(entries) = std::fs::read_dir(&directory) else {
            continue;
        };
        if listed {
            scan.directories.push(directory.clone());
        }
        for entry in entries.flatten() {
            visited += 1;
            if visited > cap {
                return None;
            }
            let candidate = entry.path();
            if depth < max_depth && entry.file_type().is_ok_and(|kind| kind.is_dir()) {
                pending.push((candidate.clone(), max_depth, depth + 1));
            }
            if !listed || !forbidden_interpreter(&candidate) {
                continue;
            }
            let Ok(metadata) = std::fs::metadata(&candidate) else {
                continue;
            };
            if metadata.is_file() && metadata.len() <= 512 * 1024 * 1024 {
                scan.files.push((candidate, metadata.len()));
            }
        }
    }
    Some(scan)
}

/// Whether any scanned interpreter has exactly this size and digest.
fn scan_matches(scan: &InterpreterScan, digest: &str, size: u64) -> bool {
    scan.files.iter().any(|(path, length)| {
        *length == size
            && std::fs::read(path)
                .ok()
                .is_some_and(|bytes| fs::sha256(&bytes) == digest)
    })
}

/// Identity and modification time of a scanned directory; `None` if unreadable.
type DirectoryStamp = Option<(u64, u64, i64, i64)>;

fn directory_stamp(path: &Path) -> DirectoryStamp {
    use std::os::unix::fs::MetadataExt;
    let metadata = std::fs::metadata(path).ok()?;
    Some((
        metadata.dev(),
        metadata.ino(),
        metadata.mtime(),
        metadata.mtime_nsec(),
    ))
}

struct CachedScan {
    roots: Vec<(PathBuf, usize)>,
    stamps: Vec<(PathBuf, DirectoryStamp)>,
    built: std::time::Instant,
    scan: Option<std::sync::Arc<InterpreterScan>>,
}

impl CachedScan {
    /// A scan stays valid while its roots and every listed directory keep the
    /// same identity and mtime and it is younger than `SCAN_TTL`.
    fn current(&self, roots: &[(PathBuf, usize)]) -> bool {
        self.roots == roots
            && self.built.elapsed() < SCAN_TTL
            && self
                .stamps
                .iter()
                .all(|(path, stamp)| directory_stamp(path) == *stamp)
    }
}

static SCAN_CACHE: std::sync::Mutex<Option<CachedScan>> = std::sync::Mutex::new(None);

/// The installed-interpreter index for `roots`, built once and reused until a
/// scanned directory changes. `None` is a cached exhausted scan (refusal).
fn cached_scan(roots: &[(PathBuf, usize)]) -> Option<std::sync::Arc<InterpreterScan>> {
    let mut cache = SCAN_CACHE
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if let Some(cached) = cache.as_ref().filter(|cached| cached.current(roots)) {
        return cached.scan.clone();
    }
    let scan = scan_interpreters(roots, SCAN_ENTRY_CAP).map(std::sync::Arc::new);
    let mut stamps: Vec<(PathBuf, DirectoryStamp)> = roots
        .iter()
        .map(|(path, _)| (path.clone(), directory_stamp(path)))
        .collect();
    if let Some(scan) = &scan {
        stamps.extend(
            scan.directories
                .iter()
                .map(|path| (path.clone(), directory_stamp(path))),
        );
    }
    *cache = Some(CachedScan {
        roots: roots.to_vec(),
        stamps,
        built: std::time::Instant::now(),
        scan: scan.clone(),
    });
    scan
}

/// Reject renamed copies of installed common interpreters as well as their
/// ordinary names. The operator still pins and reviews the actual native build.
/// Interpreters are found in `bin`-style directories of the installed roots;
/// the index is cached because every peer verification asks this question.
pub fn forbidden_interpreter_build(path: &Path, digest: &str, size: u64) -> bool {
    if forbidden_interpreter(path) {
        return true;
    }
    // Depths cover /opt/<tool>/<name>/<version>/<arch>/bin toolchains,
    // /usr/lib/<jvm>/<home>/bin and ~/.nvm/versions/node/<version>/bin.
    let mut roots: Vec<(PathBuf, usize)> = ["/bin", "/usr/bin", "/usr/local/bin"]
        .into_iter()
        .map(|path| (path.into(), 0))
        .collect();
    roots.extend([("/opt".into(), 5), ("/usr/lib".into(), 3)]);
    if let Some(home) = std::env::var_os("HOME") {
        roots.push((PathBuf::from(home).join(".nvm"), 4));
    }
    match cached_scan(&roots) {
        Some(scan) => scan_matches(&scan, digest, size),
        None => true,
    }
}

/// Uncached scan of `roots` for an interpreter build with this size and digest;
/// an exhausted scan counts as a match so the caller refuses.
#[cfg(test)]
fn interpreter_in_roots(roots: Vec<(PathBuf, usize)>, digest: &str, size: u64) -> bool {
    match scan_interpreters(&roots, SCAN_ENTRY_CAP) {
        Some(scan) => scan_matches(&scan, digest, size),
        None => true,
    }
}

fn preload_file_safe(path: &Path) -> bool {
    use std::io::Read;
    let file = match std::fs::File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return true,
        Err(_) => return false,
    };
    let mut bytes = Vec::new();
    file.take(65_537).read_to_end(&mut bytes).is_ok()
        && bytes.len() <= 65_536
        && bytes.iter().all(u8::is_ascii_whitespace)
}

/// A global loader preload undermines executable-build identity; fail closed.
pub fn loader_preload_safe() -> bool {
    preload_file_safe(Path::new("/etc/ld.so.preload"))
}

/// Stable native launch settings for one supported harness, independent of
/// provider aliases, account selection, and canonical role grants.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HarnessConfig {
    /// Existing native CLI executable.
    pub binary: PathBuf,
    /// Native state root; per-run homes are derived by the session consumer.
    pub home: PathBuf,
    /// Optional harness-wide active process cap.
    #[serde(default)]
    pub max_active_agents: Option<usize>,
    /// Forces Fast for new Codex executions across all models/providers and resumed executions.
    /// Omitted/false keeps per-request behavior; omitted serialization preserves old snapshot digests.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub fast_mode: bool,
    /// Unowned native settings subject to existing reserved-key checks.
    #[serde(default)]
    pub native_settings: BTreeMap<String, toml::Value>,
    /// Existing adapter hook declarations.
    #[serde(default)]
    pub hooks: Vec<Hook>,
    /// Existing plugin roots.
    #[serde(default)]
    pub plugins: Vec<PathBuf>,
    /// Explicit plugin asset allowlists.
    #[serde(default)]
    pub plugin_snapshot_assets: BTreeMap<String, Vec<String>>,
    /// Operator-authorized Codex workspace roots.
    #[serde(default)]
    pub workspace_roots: Vec<PathBuf>,
    /// Enables network only with Codex workspace roots.
    #[serde(default)]
    pub workspace_network: bool,
    /// Existing environment name, not an inline credential.
    #[serde(default)]
    pub environment: Option<String>,
    /// Existing Rust toolchain paths for materialization.
    #[serde(default)]
    pub rust: Option<RustRoots>,
}

/// TOML-owned provider metadata; account identities are resolved separately.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderSettings {
    /// Exactly one configured execution harness.
    pub harness: HarnessId,
    /// Native login or explicit custom HTTP gateway.
    pub connection: ProviderConnection,
    /// Protocol credential family, matched against harness and accounts.
    pub auth_family: AuthFamily,
    /// Explicit model offerings and native model aliases.
    pub models: Vec<ProviderModel>,
    /// Provider-local labels referring to global account identities.
    pub bindings: Vec<ProviderBinding>,
    /// Select eligible account bindings in descending configured weight order.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub ordered_accounts: bool,
    /// Plain-text provider advice, never a model classification.
    #[serde(default)]
    pub recommendations: Vec<String>,
    /// Positive provider ranking weight, default one.
    #[serde(default = "one")]
    pub priority_multiplier: PositiveFinite,
    /// Explicit collector selection for capacity observations.
    pub limits_source: LimitsSource,
    /// External command binding; required for the exec source.
    #[serde(default)]
    pub collector: Option<CollectorBinding>,
}

/// Opt-in selector for the dedicated no-shell managed role. No historical
/// configuration gains this field or a managed route when it is absent.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManagedAttemptBinding {
    #[serde(default)]
    pub enabled: bool,
    pub role_name: String,
    pub pool_server: String,
    pub pool_executable_sha256: String,
    pub bridge_hook_sha256: String,
    pub host_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pool_resident_executable: Option<PathBuf>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pool_resident_sha256: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transport: Option<ManagedAttemptTransport>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub bridge_receipt_readers: Vec<BridgeReceiptReader>,
    /// Separately scoped Bridge principals permitted effect recovery only.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub bridge_custody_readers: Vec<BridgeCustodyReader>,
}

/// Distinct, pinned Bridge principal permitted only the R2 receipt lookup.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BridgeReceiptReader {
    pub reader_id: String,
    pub worker_uid: u32,
    pub executable: PathBuf,
    pub executable_sha256: String,
    pub run_route: agent_run_domain::attempt_binding::Route,
    pub pool_route: agent_run_domain::attempt_binding::Route,
}

/// One separately pinned Bridge effect custody principal and its scoped grants.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BridgeCustodyReader {
    pub reader_id: String,
    pub worker_uid: u32,
    pub executable: PathBuf,
    pub executable_sha256: String,
    pub run_route: agent_run_domain::attempt_binding::Route,
    pub bridge_route: agent_run_domain::attempt_binding::Route,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub producer_scopes: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub producer_scope_prefixes: Vec<String>,
}

fn valid_scope_prefix(prefix: &str) -> bool {
    prefix.len() < 256
        && agent_run_domain::attempt_binding::validate_id(prefix).is_ok()
        && prefix.strip_suffix('/').is_some_and(|path| {
            !path.is_empty() && path.split('/').all(|segment| !segment.is_empty())
        })
}
fn scope_below_prefix(scope: &str, prefix: &str) -> bool {
    scope.strip_prefix(prefix).is_some_and(|remainder| {
        !remainder.is_empty() && remainder.split('/').all(|segment| !segment.is_empty())
    })
}
impl BridgeCustodyReader {
    /// Exact literals or a nonempty, well-formed descendant of an owned prefix.
    pub fn authorizes_scope(&self, scope: &str) -> bool {
        agent_run_domain::attempt_binding::validate_id(scope).is_ok()
            && (self.producer_scopes.iter().any(|literal| literal == scope)
                || self
                    .producer_scope_prefixes
                    .iter()
                    .any(|prefix| valid_scope_prefix(prefix) && scope_below_prefix(scope, prefix)))
    }
}

/// Pinned private Pool/Bridge routes and credential reference for R2.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManagedAttemptTransport {
    pub run_route: agent_run_domain::attempt_binding::Route,
    pub pool_route: agent_run_domain::attempt_binding::Route,
    pub bridge_route: agent_run_domain::attempt_binding::Route,
    pub producer: String,
    pub pool_socket: PathBuf,
    pub bridge_socket: PathBuf,
    pub bridge_executable: PathBuf,
    pub bridge_executable_sha256: String,
    pub bridge_reader_credential_file: PathBuf,
    pub bridge_reader_credential_sha256: String,
}

/// Returns the validated default ranking multiplier.
fn one() -> PositiveFinite {
    PositiveFinite::try_from(1.0).expect("one is positive")
}

/// Parsed v2 configuration with required legacy harnesses, optional OpenCode and arbitrary providers.
///
/// The v1 `Config` remains the current consumer contract until its launch
/// paths cut over. This type owns no mutable configuration cache or secrets.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderConfig {
    /// Must be exactly two.
    pub schema_version: u32,
    /// Explicit opt-in; absent means no managed selector or capability.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub managed_attempt_binding: Option<Box<ManagedAttemptBinding>>,
    /// Existing service lifecycle controls.
    #[serde(default)]
    pub core: Core,
    /// Existing collection controls.
    #[serde(default)]
    pub capacity: Capacity,
    /// Existing notification controls.
    #[serde(default)]
    pub delivery: Delivery,
    /// Canonical role catalog root.
    #[serde(default)]
    pub profiles: Catalog,
    /// Canonical skill catalog root.
    #[serde(default)]
    pub skills: Catalog,
    /// Role-accessible MCP definitions.
    #[serde(default)]
    pub mcp: BTreeMap<String, Mcp>,
    /// Foreground background services owned by the broker rather than any harness.
    /// Empty is omitted so historical frozen configurations keep their original digest.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub services: BTreeMap<String, crate::services::ManagedService>,
    /// Named nonsecret environment defaults.
    #[serde(default)]
    pub environments: BTreeMap<String, Environment>,
    /// Codex and Claude Code declarations plus optional OpenCode; no
    /// declarations with no providers is the explicitly empty catalog.
    #[serde(default)]
    pub harnesses: BTreeMap<HarnessId, HarnessConfig>,
    /// Arbitrary named providers, resolved against the account registry.
    #[serde(default)]
    pub providers: BTreeMap<ProviderId, ProviderSettings>,
}

impl ProviderConfig {
    /// Returns the validated canonical profile directory.
    pub fn profiles_dir(&self) -> &Path {
        self.profiles
            .directory
            .as_deref()
            .expect("validated provider config")
    }

    /// Returns the validated canonical skill directory.
    pub fn skills_dir(&self) -> &Path {
        self.skills
            .directory
            .as_deref()
            .expect("validated provider config")
    }

    /// Parses and validates v2 TOML without writes or account-store access.
    ///
    /// `home` supplies defaults for existing role/skill catalogs. Cross
    /// references to registered accounts are checked by `resolve_catalog`.
    pub fn parse(text: &str, home: &Path) -> Result<Self> {
        let mut config: Self = toml::from_str(text)
            .map_err(|_| invalid("invalid v2 configuration shape, type, or unknown field"))?;
        config.validate(home)?;
        Ok(config)
    }

    /// The shared service controls as a runtime-free schema-1 view, for
    /// operator surfaces (doctor, launchd rendering) common to both schemas.
    pub fn shared(&self) -> Config {
        Config {
            schema_version: 1,
            core: self.core.clone(),
            capacity: self.capacity.clone(),
            delivery: self.delivery.clone(),
            profiles: self.profiles.clone(),
            skills: self.skills.clone(),
            mcp: self.mcp.clone(),
            environments: self.environments.clone(),
            runtimes: BTreeMap::new(),
        }
    }

    /// Reads at most one MiB of `config.toml`; returns `None` for the same
    /// byte revision and never replaces a caller's last valid config on error.
    pub fn load_if_changed(home: &Path, revision: Option<&str>) -> Result<Option<(Self, String)>> {
        let bytes = fs::Dir::open(home)?.read(Path::new("config.toml"), 1024 * 1024)?;
        let digest = fs::sha256(&bytes);
        if revision == Some(digest.as_str()) {
            return Ok(None);
        }
        let text = std::str::from_utf8(&bytes).map_err(|_| invalid("config must be UTF-8"))?;
        Ok(Some((Self::parse(text, home)?, digest)))
    }

    /// Loads one valid v2 file and its exact-byte revision.
    pub fn load(home: &Path) -> Result<(Self, String)> {
        Self::load_if_changed(home, None)?
            .ok_or_else(|| invalid("configuration unexpectedly matched an absent revision"))
    }

    /// Validates shared v1 controls using their existing rules and checks all
    /// v2 harness and provider declarations without consulting account state.
    ///
    /// A file with neither harnesses nor providers is the explicitly empty
    /// catalog a fresh `init` writes: valid, with nothing to start or collect.
    pub fn validate(&mut self, home: &Path) -> Result<()> {
        if let Some(managed) = &self.managed_attempt_binding {
            let name = |s: &str| {
                !s.is_empty()
                    && s.len() <= 128
                    && s.bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b"_.-".contains(&b))
            };
            if !name(&managed.role_name)
                || !name(&managed.pool_server)
                || !name(&managed.host_id)
                || managed
                    .pool_executable_sha256
                    .parse::<agent_run_domain::Sha256Digest>()
                    .is_err()
                || managed
                    .bridge_hook_sha256
                    .parse::<agent_run_domain::Sha256Digest>()
                    .is_err()
            {
                return Err(invalid("invalid managed attempt selector"));
            }
            if managed.bridge_receipt_readers.len() > 32
                || managed.bridge_custody_readers.len() > 32
            {
                return Err(invalid("too many managed receipt readers"));
            }
            if managed.pool_resident_sha256.as_ref() == Some(&managed.bridge_hook_sha256) {
                return Err(invalid("managed component identity is ambiguous"));
            }
            if (!managed.bridge_receipt_readers.is_empty()
                || !managed.bridge_custody_readers.is_empty())
                && (managed
                    .pool_resident_executable
                    .as_ref()
                    .is_none_or(|path| !path.is_absolute() || forbidden_interpreter(path))
                    || managed.pool_resident_sha256.as_ref().is_none_or(|digest| {
                        digest.parse::<agent_run_domain::Sha256Digest>().is_err()
                    }))
            {
                return Err(invalid("managed Pool principal is not pinned"));
            }
            if let Some(transport) = &managed.transport {
                transport.run_route.validate()?;
                transport.pool_route.validate()?;
                transport.bridge_route.validate()?;
                agent_run_domain::attempt_binding::validate_uuid(
                    &transport.bridge_route.component_store_id,
                )?;
                let absolute = |p: &Path| {
                    p.is_absolute()
                        && !p.components().any(|c| {
                            matches!(
                                c,
                                std::path::Component::ParentDir | std::path::Component::CurDir
                            )
                        })
                };
                if managed
                    .pool_resident_executable
                    .as_deref()
                    .is_none_or(|path| !absolute(path) || forbidden_interpreter(path))
                    || managed
                        .pool_resident_sha256
                        .as_ref()
                        .is_none_or(|s| s.parse::<agent_run_domain::Sha256Digest>().is_err())
                    || !name(&transport.producer)
                    || !absolute(&transport.pool_socket)
                    || !absolute(&transport.bridge_socket)
                    || !absolute(&transport.bridge_executable)
                    || forbidden_interpreter(&transport.bridge_executable)
                    || !absolute(&transport.bridge_reader_credential_file)
                    || transport
                        .bridge_executable_sha256
                        .parse::<agent_run_domain::Sha256Digest>()
                        .is_err()
                    || transport
                        .bridge_reader_credential_sha256
                        .parse::<agent_run_domain::Sha256Digest>()
                        .is_err()
                    || managed.pool_resident_executable.as_ref()
                        == Some(&transport.bridge_executable)
                    || managed.pool_resident_sha256.as_ref()
                        == Some(&transport.bridge_executable_sha256)
                    || managed.pool_resident_sha256.as_ref() == Some(&managed.bridge_hook_sha256)
                    || transport.bridge_executable_sha256 == managed.bridge_hook_sha256
                    || transport
                        .run_route
                        .component_store_id
                        .parse::<agent_run_domain::StoreId>()
                        .is_err()
                    || transport.run_route.worker_uid != transport.pool_route.worker_uid
                    || transport.run_route.worker_uid != transport.bridge_route.worker_uid
                {
                    return Err(invalid("invalid managed attempt transport"));
                }
            }
            let mut ids = std::collections::BTreeSet::new();
            let mut paths = std::collections::BTreeSet::new();
            let mut digests = std::collections::BTreeSet::new();
            for reader in &managed.bridge_receipt_readers {
                reader.run_route.validate()?;
                reader.pool_route.validate()?;
                if !name(&reader.reader_id)
                    || !reader.executable.is_absolute()
                    || forbidden_interpreter(&reader.executable)
                    || reader.executable.components().any(|p| {
                        matches!(
                            p,
                            std::path::Component::ParentDir | std::path::Component::CurDir
                        )
                    })
                    || reader
                        .executable_sha256
                        .parse::<agent_run_domain::Sha256Digest>()
                        .is_err()
                    || reader
                        .run_route
                        .component_store_id
                        .parse::<agent_run_domain::StoreId>()
                        .is_err()
                    || reader.worker_uid != reader.run_route.worker_uid
                    || reader.worker_uid != reader.pool_route.worker_uid
                    || managed.pool_resident_executable.as_ref() == Some(&reader.executable)
                    || managed.pool_resident_sha256.as_ref() == Some(&reader.executable_sha256)
                    || managed.bridge_hook_sha256 == reader.executable_sha256
                    || !ids.insert(&reader.reader_id)
                    || !paths.insert(&reader.executable)
                    || !digests.insert(&reader.executable_sha256)
                {
                    return Err(invalid("invalid managed receipt reader"));
                }
            }
            let mut custody_ids = std::collections::BTreeSet::new();
            let mut custody_paths = std::collections::BTreeSet::new();
            let mut custody_digests = std::collections::BTreeSet::new();
            let mut scopes = std::collections::BTreeSet::new();
            let mut prefixes: Vec<(&str, usize)> = Vec::new();
            let mut literals: Vec<(&str, usize)> = Vec::new();
            for (owner, reader) in managed.bridge_custody_readers.iter().enumerate() {
                reader.run_route.validate()?;
                reader.bridge_route.validate()?;
                agent_run_domain::attempt_binding::validate_uuid(
                    &reader.bridge_route.component_store_id,
                )?;
                if !name(&reader.reader_id)
                    || !reader.executable.is_absolute()
                    || forbidden_interpreter(&reader.executable)
                    || reader.executable.components().any(|p| {
                        matches!(
                            p,
                            std::path::Component::ParentDir | std::path::Component::CurDir
                        )
                    })
                    || reader
                        .executable_sha256
                        .parse::<agent_run_domain::Sha256Digest>()
                        .is_err()
                    || reader
                        .run_route
                        .component_store_id
                        .parse::<agent_run_domain::StoreId>()
                        .is_err()
                    || reader.worker_uid != reader.run_route.worker_uid
                    || reader.worker_uid != reader.bridge_route.worker_uid
                    || managed.pool_resident_executable.as_ref() == Some(&reader.executable)
                    || managed.pool_resident_sha256.as_ref() == Some(&reader.executable_sha256)
                    || managed.bridge_hook_sha256 == reader.executable_sha256
                    || !custody_ids.insert(&reader.reader_id)
                    || !custody_paths.insert(&reader.executable)
                    || !custody_digests.insert(&reader.executable_sha256)
                    || (reader.producer_scopes.is_empty()
                        && reader.producer_scope_prefixes.is_empty())
                    || reader
                        .producer_scopes
                        .len()
                        .saturating_add(reader.producer_scope_prefixes.len())
                        > 128
                    || reader.producer_scopes.iter().any(|scope| {
                        agent_run_domain::attempt_binding::validate_id(scope).is_err()
                            || !scopes.insert(scope)
                    })
                    || reader
                        .producer_scope_prefixes
                        .iter()
                        .any(|prefix| !valid_scope_prefix(prefix))
                    || managed.bridge_receipt_readers.iter().any(|receipt| {
                        (receipt.executable == reader.executable)
                            != (receipt.executable_sha256 == reader.executable_sha256)
                    })
                {
                    return Err(invalid("invalid managed custody reader"));
                }
                literals.extend(
                    reader
                        .producer_scopes
                        .iter()
                        .map(|scope| (scope.as_str(), owner)),
                );
                prefixes.extend(
                    reader
                        .producer_scope_prefixes
                        .iter()
                        .map(|prefix| (prefix.as_str(), owner)),
                );
            }
            for (index, (prefix, owner)) in prefixes.iter().enumerate() {
                if prefixes[..index]
                    .iter()
                    .any(|(other, _)| prefix.starts_with(*other) || other.starts_with(*prefix))
                    || literals.iter().any(|(scope, literal_owner)| {
                        literal_owner != owner && scope_below_prefix(scope, prefix)
                    })
                {
                    return Err(invalid("overlapping managed custody scopes"));
                }
            }
        }
        if self.services.len() > 32 {
            return Err(invalid("at most 32 managed services may be configured"));
        }
        for (id, service) in &self.services {
            if !crate::services::valid_id(id) {
                return Err(invalid("invalid managed service id"));
            }
            service.validate()?;
        }
        let empty = self.harnesses.is_empty() && self.providers.is_empty();
        if self.schema_version != 2
            || (!empty
                && (!self.harnesses.contains_key(&HarnessId::Codex)
                    || !self.harnesses.contains_key(&HarnessId::ClaudeCode)
                    || self.providers.is_empty()))
        {
            return Err(invalid(
                "v2 requires codex and claude-code harnesses and providers, or neither",
            ));
        }
        // CodexBar is not a schema-2 source; its binary is migration input only.
        if self.capacity.legacy_codexbar_binary.is_some() {
            return Err(invalid(
                "capacity.codexbar_binary is retired in schema 2; remove it",
            ));
        }
        // MCP tool controls are schema-2 features that the shared schema-1
        // pass rejects outright, so their lists are validated here and the
        // controls are stripped from the shared view, then restored onto the
        // expanded declarations below.
        let controls: BTreeMap<String, (bool, Option<Vec<String>>)> = self
            .mcp
            .iter()
            .map(|(id, server)| (id.clone(), (server.global, server.allowed_tools.clone())))
            .collect();
        for (_, tools) in controls.values() {
            if let Some(tools) = tools {
                config::validate_allowed_tools("MCP allowed_tools", tools)?;
            }
        }
        let mut shared = Config {
            schema_version: 1,
            core: self.core.clone(),
            capacity: self.capacity.clone(),
            delivery: self.delivery.clone(),
            profiles: self.profiles.clone(),
            skills: self.skills.clone(),
            mcp: self
                .mcp
                .iter()
                .map(|(id, server)| {
                    let mut stripped = server.clone();
                    stripped.global = false;
                    stripped.allowed_tools = None;
                    (id.clone(), stripped)
                })
                .collect(),
            environments: self.environments.clone(),
            runtimes: BTreeMap::new(),
        };
        shared.validate(home)?;
        self.core = shared.core;
        self.capacity = shared.capacity;
        self.delivery = shared.delivery;
        self.profiles = shared.profiles;
        self.skills = shared.skills;
        self.mcp = shared.mcp;
        self.environments = shared.environments;
        for (id, (global, allowed_tools)) in controls {
            let server = self
                .mcp
                .get_mut(&id)
                .expect("schema-1 pass preserves MCP ids");
            server.global = global;
            server.allowed_tools = allowed_tools;
        }
        for (id, harness) in &mut self.harnesses {
            harness.binary = fs::expand(&harness.binary)?;
            harness.home = fs::expand(&harness.home)?;
            if !harness.binary.is_absolute() || !harness.home.is_absolute() {
                return Err(invalid("harness binary and home must be absolute"));
            }
            if harness.fast_mode && *id != HarnessId::Codex {
                return Err(invalid("harness fast_mode is supported only by codex"));
            }
            if harness.max_active_agents == Some(0) {
                return Err(invalid("harness max_active_agents must be positive"));
            }
            let adapter = match id {
                HarnessId::Codex => config::Adapter::Codex,
                HarnessId::ClaudeCode => config::Adapter::Claude,
                HarnessId::OpenCode => config::Adapter::OpenCode,
            };
            config::native_settings(adapter, &harness.native_settings)?;
            if harness.workspace_network
                && (*id != HarnessId::Codex || harness.workspace_roots.is_empty())
            {
                return Err(invalid("workspace_network requires codex workspace_roots"));
            }
            if !harness.workspace_roots.is_empty() && *id != HarnessId::Codex {
                return Err(invalid("workspace_roots requires codex"));
            }
            for root in &mut harness.workspace_roots {
                *root = fs::expand(root)?;
                if !root.is_absolute() {
                    return Err(invalid("workspace root must be absolute"));
                }
            }
            if harness
                .environment
                .as_ref()
                .is_some_and(|name| !self.environments.contains_key(name))
            {
                return Err(invalid("unknown harness environment"));
            }
            for hook in &harness.hooks {
                if hook.event.trim().is_empty()
                    || hook.command.is_empty()
                    || hook
                        .command
                        .iter()
                        .any(|arg| arg.is_empty() || arg.contains('\0'))
                {
                    return Err(invalid("invalid harness hook"));
                }
            }
            for plugin in &mut harness.plugins {
                *plugin = fs::expand(plugin)?;
                if !plugin.is_dir() {
                    return Err(invalid("plugin must be an existing directory"));
                }
            }
            for (plugin, assets) in &harness.plugin_snapshot_assets {
                if harness
                    .plugins
                    .iter()
                    .filter(|path| path.file_name().is_some_and(|name| name == plugin.as_str()))
                    .count()
                    != 1
                    || assets.is_empty()
                    || assets.iter().any(|asset| {
                        fs::relative(Path::new(asset)).is_err()
                            || asset.chars().any(|c| "*?[]{}".contains(c))
                    })
                    || assets
                        .iter()
                        .collect::<std::collections::BTreeSet<_>>()
                        .len()
                        != assets.len()
                {
                    return Err(invalid("invalid harness plugin assets"));
                }
            }
            if let Some(roots) = &mut harness.rust {
                roots.rustup_home = fs::expand(&roots.rustup_home)?;
                roots.cargo_bin = fs::expand(&roots.cargo_bin)?;
            }
        }
        for (id, provider) in &self.providers {
            if matches!(
                provider.limits_source,
                LimitsSource::Lua | LimitsSource::CodexAppserver
            ) {
                return Err(invalid(
                    "built-in quota collectors are retired; configure limits_source = exec and collector.command",
                ));
            }
            if !self.harnesses.contains_key(&provider.harness) {
                return Err(invalid("provider names an undeclared harness"));
            }
            if provider.models.iter().any(|model| {
                model
                    .params
                    .keys()
                    .chain(model.allowed_params.keys())
                    .any(|key| key != "effort")
            }) {
                return Err(invalid("unsupported model parameter"));
            }
            self.definition(id, provider).validate()?;
        }
        Ok(())
    }

    /// Resolves TOML-owned providers against explicit registered account
    /// records, rejecting missing, duplicate, or auth-incompatible bindings.
    pub fn resolve_catalog(&self, accounts: Vec<AccountRecord>) -> Result<ProviderCatalog> {
        ProviderCatalog::new(
            accounts,
            self.providers
                .iter()
                .map(|(id, provider)| self.definition(id, provider))
                .collect(),
        )
    }

    /// Returns a metadata-only digest and stable configured ids for this validated
    /// configuration. Environment, arguments, references and credentials are hashed
    /// but never emitted; historical verification uses the same raw-document rule.
    pub fn snapshot(&self) -> Result<Value> {
        Self::snapshot_document(&serde_json::to_value(self)?)
    }

    /// Summarizes a complete serialized configuration without decoding retired
    /// execution controls. `document` must have object-valued harness/provider maps;
    /// malformed shape returns ValidationError. Its original canonical bytes define
    /// the digest, preserving historical verification before in-memory projection.
    /// Emits only schema, digest and sorted ids, never stored settings or secrets.
    pub fn snapshot_document(document: &Value) -> Result<Value> {
        let harnesses = document
            .get("harnesses")
            .and_then(Value::as_object)
            .ok_or_else(|| invalid("serialized harness configuration is malformed"))?;
        let providers = document
            .get("providers")
            .and_then(Value::as_object)
            .ok_or_else(|| invalid("serialized provider configuration is malformed"))?;
        Ok(json!({
            "schema_version":2,
            "sha256":canonical::sha256_hex(document,true),
            "harnesses":harnesses.keys().collect::<Vec<_>>(),
            "providers":providers.keys().collect::<Vec<_>>(),
        }))
    }

    /// Builds one public provider definition without reading account state.
    fn definition(&self, id: &ProviderId, provider: &ProviderSettings) -> ProviderDefinition {
        ProviderDefinition {
            id: id.clone(),
            harness: provider.harness,
            connection: provider.connection.clone(),
            auth_family: provider.auth_family.clone(),
            recommendations: provider.recommendations.clone(),
            priority_multiplier: provider.priority_multiplier,
            limits_source: provider.limits_source,
            collector: provider.collector.clone(),
            models: provider.models.clone(),
            bindings: provider.bindings.clone(),
            ordered_accounts: provider.ordered_accounts,
        }
    }
}

#[cfg(test)]
mod managed_peer_tests {
    use super::*;

    #[test]
    fn managed_pins_reject_shared_builds_interpreters_and_ambiguous_custody_scopes() {
        let config = format!(
            r#"schema_version=2
[managed_attempt_binding]
enabled=true
role_name='managed'
pool_server='pool'
pool_executable_sha256='{frontend}'
bridge_hook_sha256='{hook}'
host_id='host'
pool_resident_executable='/opt/pool-resident'
pool_resident_sha256='{resident}'
[managed_attempt_binding.transport]
run_route={{component_store_id='store_0123456789abcdef0123456789abcdef',route_id='run',worker_uid=1000}}
pool_route={{component_store_id='pool-store',route_id='pool',worker_uid=1000}}
bridge_route={{component_store_id='00000000-0000-4000-8000-000000000001',route_id='bridge',worker_uid=1000}}
producer='pool'
pool_socket='/opt/pool.sock'
bridge_socket='/opt/bridge.sock'
bridge_executable='/opt/bridge-daemon'
bridge_executable_sha256='{bridge}'
bridge_reader_credential_file='/opt/reader'
bridge_reader_credential_sha256='{credential}'
[[managed_attempt_binding.bridge_custody_readers]]
reader_id='custody'
worker_uid=1000
executable='/opt/bridge-daemon'
executable_sha256='{bridge}'
run_route={{component_store_id='store_0123456789abcdef0123456789abcdef',route_id='run',worker_uid=1000}}
bridge_route={{component_store_id='00000000-0000-4000-8000-000000000001',route_id='bridge',worker_uid=1000}}
producer_scopes=['scope']
"#,
            frontend = "a".repeat(64),
            hook = "c".repeat(64),
            resident = "b".repeat(64),
            bridge = "d".repeat(64),
            credential = "e".repeat(64)
        );
        let parse = |text: &str| ProviderConfig::parse(text, Path::new("/fixture"));
        assert!(parse(&config).is_ok());
        // The frontend and resident are separate process roles of one build.
        assert!(
            parse(&config.replace(
                &format!("pool_resident_sha256='{}'", "b".repeat(64)),
                &format!("pool_resident_sha256='{}'", "a".repeat(64)),
            ))
            .is_ok()
        );
        for invalid in [
            config.replacen(
                &format!("bridge_executable_sha256='{}'", "d".repeat(64)),
                &format!("bridge_executable_sha256='{}'", "c".repeat(64)),
                1,
            ),
            config.replacen(
                &format!("pool_resident_sha256='{}'", "b".repeat(64)),
                &format!("pool_resident_sha256='{}'", "c".repeat(64)),
                1,
            ),
            config.replace("/opt/bridge-daemon", "/usr/bin/python3"),
            config.replace("/opt/bridge-daemon", "/usr/bin/nodejs"),
            config.replace("/opt/bridge-daemon", "/usr/bin/pypy3"),
            config.replace(
                "producer_scopes=['scope']",
                "producer_scopes=['scope','scope']",
            ),
            config.replace(
                "producer_scopes=['scope']",
                "producer_scope_prefixes=['pool/store/pool']",
            ),
            config.replace(
                "producer_scopes=['scope']",
                "producer_scope_prefixes=['pool//pool/']",
            ),
            config.replace(
                "producer_scopes=['scope']",
                "producer_scope_prefixes=['pool/store/','pool/store/member/']",
            ),
        ] {
            assert!(parse(&invalid).is_err());
        }
        let with_prefix = config.replace(
            "producer_scopes=['scope']",
            "producer_scopes=['scope']\nproducer_scope_prefixes=['pool/store/pool/']",
        );
        let mut parsed = parse(&with_prefix).unwrap();
        let settings = parsed.managed_attempt_binding.as_ref().unwrap();
        let reader = &settings.bridge_custody_readers[0];
        for scope in [
            "scope",
            "pool/store/pool/member",
            "pool/store/pool/member/final-notice",
        ] {
            assert!(reader.authorizes_scope(scope), "{scope}");
        }
        for scope in [
            "pool/store/pool/",
            "pool/store/pool",
            "pool/store/poolish/member",
            "pool/store/pool//member",
            "pool/store/pool/member/",
        ] {
            assert!(!reader.authorizes_scope(scope), "{scope}");
        }
        assert!(
            parse(&config.replace(
                "producer_scopes=['scope']",
                "producer_scope_prefixes=['pool/store/pool/']"
            ))
            .is_ok()
        );
        let mut other = reader.clone();
        other.reader_id = "other".into();
        other.executable = "/opt/other-bridge".into();
        other.executable_sha256 = "f".repeat(64);
        other.producer_scopes = vec!["pool/store/pool/member".into()];
        other.producer_scope_prefixes.clear();
        parsed
            .managed_attempt_binding
            .as_mut()
            .unwrap()
            .bridge_custody_readers
            .push(other);
        assert!(parsed.validate(Path::new("/fixture")).is_err());
        let other = parsed
            .managed_attempt_binding
            .as_mut()
            .unwrap()
            .bridge_custody_readers
            .last_mut()
            .unwrap();
        other.producer_scopes.clear();
        other.producer_scope_prefixes = vec!["pool/store/pool/member/".into()];
        assert!(parsed.validate(Path::new("/fixture")).is_err());
        parsed
            .managed_attempt_binding
            .as_mut()
            .unwrap()
            .bridge_custody_readers
            .last_mut()
            .unwrap()
            .producer_scope_prefixes = vec!["pool/other/".into()];
        assert!(parsed.validate(Path::new("/fixture")).is_ok());
    }

    #[test]
    fn interpreter_aliases_recursive_copies_and_loader_preload_fail_closed() {
        for name in [
            "awk",
            "gawk",
            "expect",
            "Rscript",
            "julia",
            "erl",
            "escript",
            "ld.so",
            "ld-linux-x86-64.so.2",
        ] {
            assert!(
                forbidden_interpreter(&PathBuf::from("/fixture").join(name)),
                "{name}"
            );
        }
        let root = tempfile::tempdir().unwrap();
        let nested = root.path().join("versions/node/v1/bin");
        std::fs::create_dir_all(&nested).unwrap();
        let bytes = b"synthetic interpreter build";
        std::fs::write(nested.join("node"), bytes).unwrap();
        assert!(interpreter_in_roots(
            vec![(root.path().into(), 5)],
            &fs::sha256(bytes),
            bytes.len() as u64
        ));
        assert!(!interpreter_in_roots(
            vec![(root.path().into(), 5)],
            &"0".repeat(64),
            bytes.len() as u64
        ));
        let preload = root.path().join("ld.so.preload");
        assert!(preload_file_safe(&preload));
        std::fs::write(&preload, b" \n\t").unwrap();
        assert!(preload_file_safe(&preload));
        std::fs::write(&preload, b"/synthetic/library.so\n").unwrap();
        assert!(!preload_file_safe(&preload));
    }
    /// A data tree far beyond the entry cap, outside executable directories,
    /// must neither be walked nor refuse a legitimate peer; an installed
    /// interpreter under a `bin` directory is still recognised by its build.
    #[test]
    fn large_data_trees_do_not_refuse_a_legitimate_peer() {
        let root = tempfile::tempdir().unwrap();
        let deep = root.path().join("lib/a/b/c/d/e/f/data");
        std::fs::create_dir_all(&deep).unwrap();
        for index in 0..=SCAN_ENTRY_CAP + 10_000 {
            std::fs::File::create(deep.join(index.to_string())).unwrap();
        }
        let bin = root.path().join("toolchain/1.0/x64/bin");
        std::fs::create_dir_all(&bin).unwrap();
        let interpreter = b"synthetic interpreter build";
        std::fs::write(bin.join("python3"), interpreter).unwrap();
        let peer = b"legitimate peer executable";
        let roots = vec![(root.path().to_path_buf(), 5)];
        assert!(!interpreter_in_roots(
            roots.clone(),
            &fs::sha256(peer),
            peer.len() as u64
        ));
        assert!(interpreter_in_roots(
            roots,
            &fs::sha256(interpreter),
            interpreter.len() as u64
        ));
    }

    /// The scan fails closed when its own entry cap is exhausted.
    #[test]
    fn exhausted_scan_refuses() {
        let root = tempfile::tempdir().unwrap();
        for index in 0..20 {
            std::fs::File::create(root.path().join(index.to_string())).unwrap();
        }
        let roots = vec![(root.path().to_path_buf(), 0)];
        assert!(scan_interpreters(&roots, 20).is_some());
        assert!(scan_interpreters(&roots, 19).is_none());
    }

    /// The cached index is reused until a scanned directory changes.
    #[test]
    fn cached_scan_follows_directory_changes() {
        let root = tempfile::tempdir().unwrap();
        let roots = vec![(root.path().to_path_buf(), 5)];
        let first = cached_scan(&roots).unwrap();
        assert!(first.files.is_empty());
        assert!(std::sync::Arc::ptr_eq(
            &first,
            &cached_scan(&roots).unwrap()
        ));
        // Filesystem timestamps tick coarsely; let the root's mtime advance.
        std::thread::sleep(std::time::Duration::from_millis(50));
        let bin = root.path().join("tool/bin");
        std::fs::create_dir_all(&bin).unwrap();
        std::fs::write(bin.join("node"), b"node build").unwrap();
        let second = cached_scan(&roots).unwrap();
        assert_eq!(second.files.len(), 1);
    }
}
