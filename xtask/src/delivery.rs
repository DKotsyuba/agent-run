//! External release identity, scoped evidence and exact artifact inventory.
//! Separate from the legacy internal COMPLETE seal and deployment transaction.
use crate::release::{digest, digest_bytes};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    collections::BTreeSet,
    fs,
    io::{Read, Write},
    os::unix::{fs::OpenOptionsExt, process::CommandExt},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::mpsc,
    time::{Duration, Instant},
};

/// Expected immutable GitHub repository identity.
pub const REPOSITORY_ID: u64 = 1348534205;
/// Canonical official repository; caller-controlled URLs are not accepted.
pub const REPOSITORY: &str = "DKotsyuba/agent-run";
/// Reviewed immutable release workflow identity.
pub const WORKFLOW_ID: u64 = 346709607;
/// Reviewed release producer path.
pub const WORKFLOW_PATH: &str = ".github/workflows/release.yml";
/// External identity filename; never included in its own payload hash list.
pub const MANIFEST: &str = "release-manifest.json";
/// Scoped check evidence, hashed as an ordinary manifest artifact.
pub const ACCEPTANCE: &str = "acceptance.json";
/// Best-effort cancellation shared by the foreground observer and owned tools.
pub(crate) static CANCELLED: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

/// One actual GitHub workflow invocation; local builds use null instead.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Workflow {
    /// Numeric workflow ID obtained from GitHub, not inferred from its name.
    pub id: u64,
    /// Exact reviewed workflow path.
    pub path: String,
    /// Actual run identity.
    pub run_id: u64,
    /// Actual attempt, never silently switched during verification.
    pub run_attempt: u64,
}

/// Source/build identity shared by manifest and acceptance evidence.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Identity {
    /// Product identity, independent of SDK/protocol schemas.
    pub product: String,
    /// Cargo workspace version.
    pub version: String,
    /// Version-bound annotated release tag.
    pub tag: String,
    /// Canonical repository name.
    pub repository: String,
    /// Stable numeric repository identity.
    pub repository_id: u64,
    /// Full accepted source commit.
    pub commit: String,
    /// Real CI invocation, absent for explicitly local evidence.
    pub workflow: Option<Workflow>,
    /// Product delivery target; no local qualification is implied.
    pub target: String,
    /// Pinned Rust compiler declaration.
    pub toolchain: String,
    /// Reviewed family standard version.
    pub standard_version: String,
    /// Reviewed devkit version.
    pub devkit_version: String,
    /// Reviewed compatibility baseline.
    pub baseline: String,
}

/// An actual command outcome; arbitrary argv/output never enter public evidence.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Check {
    /// Fixed reviewed check category, or custom when no approved category matches.
    pub check_id: String,
    /// SHA-256 of exact executed argv; no heuristic secret scrubber is used.
    pub argv_sha256: String,
    /// Actual exit code; a nonzero result never qualifies packaging.
    pub exit_code: i32,
    /// SHA-256 of actual captured stdout.
    pub stdout_sha256: String,
    /// SHA-256 of actual captured stderr.
    pub stderr_sha256: String,
}

/// Actual scoped evidence; qualification remains a separate acceptance.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Acceptance {
    /// Current evidence format.
    pub schema_version: u64,
    /// Same source/run/attempt identity as the payload.
    pub identity: Identity,
    /// Explicit local or github-actions scope.
    pub scope: String,
    /// Completed observed checks, in execution order.
    pub checks: Vec<Check>,
    /// This producer never invents host qualification.
    pub qualified_hosts: Vec<String>,
    /// Exact bundle whose disposable installation/smoke was actually observed.
    #[serde(default)]
    pub payload: Option<Artifact>,
}

/// One exact named payload in the external inventory.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Artifact {
    /// Safe basename, never a filesystem path or URL.
    pub name: String,
    /// bundle/source/installer/evidence.
    pub kind: String,
    /// Optional target; required for the bundle.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target: Option<String>,
    /// Actual encoded bytes.
    pub size: u64,
    /// Actual SHA-256 of these bytes.
    pub sha256: String,
}

/// Published shared closed-v1 release manifest. Build-only target/toolchain
/// proof stays in hashed acceptance evidence; local builds cannot invent CI IDs.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Manifest {
    /// Shared manifest format.
    pub schema_version: u64,
    /// Canonical product.
    pub product: String,
    /// Cargo version bound to tag.
    pub version: String,
    /// Exact annotated publication tag.
    pub tag: String,
    /// Expected GitHub repository.
    pub repository: String,
    /// Stable repository identity.
    pub repository_id: u64,
    /// Full accepted commit.
    pub commit: String,
    /// Actual workflow/run/attempt; null is not a published shared-v1 identity.
    pub workflow: Workflow,
    /// Family standard version.
    pub standard_version: String,
    /// Reviewed devkit version.
    pub devkit_version: String,
    /// Reviewed compatibility baseline.
    pub baseline: String,
    /// Required publication trust profile; local inventory checks alone do not
    /// authenticate GitHub assets or verify producer attestations.
    pub trust_profile: String,
    /// Actual named artifact inventory, excluding this manifest and checksums.
    pub artifacts: Vec<Artifact>,
}

impl Manifest {
    /// Projects source fields only. Explicit local evidence remains usable for
    /// build checks but cannot become a shared published manifest with fake IDs.
    pub(crate) fn from_identity(id: &Identity, artifacts: Vec<Artifact>) -> Result<Self, String> {
        Ok(Self { schema_version: 1, product: id.product.clone(), version: id.version.clone(),
            tag: id.tag.clone(), repository: id.repository.clone(), repository_id: id.repository_id,
            commit: id.commit.clone(), workflow: id.workflow.clone().ok_or("published manifest requires actual workflow identity; local evidence is not publication")?,
            standard_version: id.standard_version.clone(), devkit_version: id.devkit_version.clone(),
            baseline: id.baseline.clone(), trust_profile: "github-attestation".into(), artifacts })
    }
}

/// Exact-child/process-identity guard for bounded external tooling.
struct Process {
    /// Unreaped owned child.
    child: Child,
    /// Existing PID-reuse-safe group/descendant evidence.
    owner: agent_run_platform::process::OwnedProcess,
    /// Cancels both owned pipe drains on early return.
    cancel: std::sync::Arc<std::sync::atomic::AtomicBool>,
    /// Owned bounded drain threads, joined on every exit.
    readers: Vec<std::thread::JoinHandle<()>>,
}
impl Drop for Process {
    /// Signals groups only through verified ownership, then reaps this exact child.
    fn drop(&mut self) {
        self.cancel
            .store(true, std::sync::atomic::Ordering::Release);
        let _ = self.owner.cleanup_blocking(Duration::from_millis(100));
        if self.child.try_wait().ok().flatten().is_none() {
            let _ = self.child.kill();
        }
        let _ = self.child.wait();
        for reader in self.readers.drain(..) {
            let _ = reader.join();
        }
    }
}

/// Executes trusted argv with a finite operation deadline and bounded streams.
/// The existing ownership guard cleans captured descendants after leader exit;
/// owned pipe workers are cancelled/joined on every return. Stdin is always null.
pub(crate) fn run(
    program: &str,
    arguments: &[String],
    cwd: &Path,
    timeout: Duration,
) -> Result<(i32, Vec<u8>, Vec<u8>), String> {
    run_io(program, arguments, cwd, timeout, None)
}

/// Executes owned bounded tooling; optional stdin is an already-created regular
/// event file. GitHub downloads additionally have a per-file 512 MiB hard limit.
/// Non-GitHub children never inherit release token environment variables.
pub(crate) fn run_io(
    program: &str,
    arguments: &[String],
    cwd: &Path,
    timeout: Duration,
    input: Option<&Path>,
) -> Result<(i32, Vec<u8>, Vec<u8>), String> {
    let deadline = Instant::now() + timeout;
    let cancel = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let mut command = Command::new(program);
    command
        .args(arguments)
        .current_dir(cwd)
        .process_group(0)
        .env_remove("AGENT_RUN_WORKER_TOKEN")
        .stdin(match input {
            Some(path) => Stdio::from(fs::File::open(path).map_err(|_| "event stdin unavailable")?),
            None => Stdio::null(),
        })
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    if program != "gh" {
        for name in [
            "GH_TOKEN",
            "GITHUB_TOKEN",
            "GH_ENTERPRISE_TOKEN",
            "GITHUB_ENTERPRISE_TOKEN",
        ] {
            command.env_remove(name);
        }
    } else {
        command.env("GH_HOST", "github.com");
        // SAFETY: setrlimit is async-signal-safe and only affects this owned child.
        unsafe {
            command.pre_exec(|| {
                let bound = libc::rlimit {
                    rlim_cur: 512 * 1024 * 1024,
                    rlim_max: 512 * 1024 * 1024,
                };
                if libc::setrlimit(libc::RLIMIT_FSIZE, &bound) != 0 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }
    }
    let child = command.spawn().map_err(|_| "external tool unavailable")?;
    let mut process = Process {
        owner: agent_run_platform::process::OwnedProcess::capture(child.id() as i32),
        child,
        cancel: cancel.clone(),
        readers: Vec::new(),
    };
    let stdout = fs::File::from(std::os::fd::OwnedFd::from(
        process.child.stdout.take().ok_or("stdout missing")?,
    ));
    let stderr = fs::File::from(std::os::fd::OwnedFd::from(
        process.child.stderr.take().ok_or("stderr missing")?,
    ));
    let (send, receive) = mpsc::channel();
    for (index, pipe) in [(0, stdout), (1, stderr)] {
        let send = send.clone();
        let cancel = cancel.clone();
        process.readers.push(std::thread::spawn(move || {
            let pipe = crate::tar_guard::TimedReader {
                pipe,
                deadline,
                cancel: Some(cancel),
            };
            let mut bytes = Vec::new();
            let result = pipe.take(4 * 1024 * 1024 + 1).read_to_end(&mut bytes);
            let _ = send.send((index, result.is_ok(), bytes));
        }));
    }
    drop(send);
    let mut streams = [None, None];
    let mut status = None;
    loop {
        while let Ok((index, ok, bytes)) = receive.try_recv() {
            if !ok || bytes.len() > 4 * 1024 * 1024 {
                return Err("external tool output limit/read failure".into());
            }
            streams[index] = Some(bytes);
        }
        process.owner.refresh();
        if status.is_none() {
            status = process.child.try_wait().map_err(|_| "tool wait failed")?;
        }
        if let Some(status) = status
            && streams.iter().all(Option::is_some)
        {
            return Ok((
                status.code().unwrap_or(1),
                streams[0].take().ok_or("stdout missing")?,
                streams[1].take().ok_or("stderr missing")?,
            ));
        }
        if CANCELLED.load(std::sync::atomic::Ordering::Relaxed) {
            return Err("cancelled".into());
        }
        if Instant::now() >= deadline {
            return Err("external tool deadline exceeded".into());
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// Runs a small read-only Git query with a finite deadline.
pub(crate) fn git(root: &Path, args: &[&str]) -> Result<String, String> {
    let args = args.iter().map(|s| (*s).into()).collect::<Vec<_>>();
    let (code, out, _) = run("git", &args, root, Duration::from_secs(15))?;
    if code != 0 {
        return Err("source identity query failed".into());
    }
    String::from_utf8(out)
        .map(|s| s.trim().to_owned())
        .map_err(|_| "invalid source identity".into())
}

/// Constructs authoritative identity only from a clean accepted source tree.
/// Local evidence is explicit; workflow/run/attempt exist only in GitHub Actions.
pub fn identity(root: &Path, accepted: &str) -> Result<Identity, String> {
    if accepted.len() != 40
        || !accepted
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err("full accepted commit required".into());
    }
    if git(root, &["rev-parse", "HEAD"])? != accepted
        || !git(root, &["status", "--porcelain", "--untracked-files=normal"])?.is_empty()
    {
        return Err("authoritative packaging requires clean accepted HEAD".into());
    }
    let cargo: Value = serde_json::to_value(
        toml::from_str::<toml::Value>(
            &fs::read_to_string(root.join("Cargo.toml")).map_err(|_| "Cargo unavailable")?,
        )
        .map_err(|_| "invalid Cargo")?,
    )
    .map_err(|_| "Cargo identity failed")?;
    let family: Value = serde_json::to_value(
        toml::from_str::<toml::Value>(
            &fs::read_to_string(root.join("family.toml")).map_err(|_| "family unavailable")?,
        )
        .map_err(|_| "invalid family")?,
    )
    .map_err(|_| "invalid family value")?;
    let version = cargo["workspace"]["package"]["version"]
        .as_str()
        .ok_or("workspace version missing")?
        .to_owned();
    let toolchain: Value = serde_json::to_value(
        toml::from_str::<toml::Value>(
            &fs::read_to_string(root.join("rust-toolchain.toml"))
                .map_err(|_| "toolchain unavailable")?,
        )
        .map_err(|_| "invalid toolchain")?,
    )
    .map_err(|_| "invalid toolchain value")?;
    let field = |name: &str| {
        family
            .get(name)
            .and_then(Value::as_str)
            .map(str::to_owned)
            .ok_or_else(|| format!("family {name} missing"))
    };
    let mut id = Identity {
        product: "agent-run".into(),
        tag: format!("v{version}"),
        version,
        repository: REPOSITORY.into(),
        repository_id: REPOSITORY_ID,
        commit: accepted.into(),
        workflow: None,
        target: "aarch64-apple-darwin".into(),
        toolchain: toolchain["toolchain"]["channel"]
            .as_str()
            .ok_or("toolchain channel missing")?
            .into(),
        standard_version: field("standard_version")?,
        devkit_version: field("devkit_version")?,
        baseline: field("baseline")?,
    };
    let (compiler_code, compiler_version, _) = run(
        "rustc",
        &["--version".into()],
        root,
        Duration::from_secs(15),
    )?;
    if compiler_code != 0
        || !String::from_utf8_lossy(&compiler_version)
            .starts_with(&format!("rustc {} ", id.toolchain))
    {
        return Err("effective compiler/pinned toolchain mismatch".into());
    }
    if family["release"]["enabled"].as_bool() != Some(true)
        || family["release"]["trust_profile"].as_str() != Some("github-attestation")
        || !family["compatibility"]["qualified_targets"]
            .as_array()
            .is_some_and(|targets| {
                targets
                    .iter()
                    .any(|target| target.as_str() == Some(&id.target))
            })
    {
        return Err("release disabled/unqualified target/trust conflict".into());
    }
    if std::env::var("GITHUB_ACTIONS").as_deref() == Ok("true") {
        let number = |name: &str| {
            std::env::var(name)
                .ok()
                .and_then(|v| v.parse::<u64>().ok())
                .filter(|n| *n > 0)
                .ok_or_else(|| format!("{name} identity missing"))
        };
        if std::env::var("GITHUB_REPOSITORY").as_deref() != Ok(REPOSITORY)
            || number("GITHUB_REPOSITORY_ID")? != REPOSITORY_ID
            || std::env::var("GITHUB_SHA").as_deref() != Ok(accepted)
            || std::env::var("GITHUB_REF_NAME").as_deref() != Ok(&id.tag)
        {
            return Err("GitHub source identity mismatch".into());
        }
        if git(root, &["cat-file", "-t", &id.tag])? != "tag"
            || git(root, &["rev-parse", &format!("{}^{{commit}}", id.tag)])? != accepted
        {
            return Err("annotated tag identity mismatch".into());
        }
        id.workflow = Some(Workflow {
            id: {
                let actual = number("AGENT_RUN_WORKFLOW_ID")?;
                if actual != WORKFLOW_ID {
                    return Err("reviewed workflow identity mismatch".into());
                }
                actual
            },
            path: ".github/workflows/release.yml".into(),
            run_id: number("GITHUB_RUN_ID")?,
            run_attempt: number("GITHUB_RUN_ATTEMPT")?,
        });
    }
    Ok(id)
}

/// Atomically publishes create-new private JSON after file fsync, then fsyncs
/// its directory. Existing files/symlinks are refused without replacing bytes.
pub(crate) fn create_json(path: &Path, value: &impl Serialize) -> Result<(), String> {
    let parent = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let mut file = tempfile::Builder::new()
        .prefix(".release-record-")
        .tempfile_in(parent)
        .map_err(|_| "evidence scratch unavailable")?;
    use std::os::unix::fs::PermissionsExt;
    file.as_file()
        .set_permissions(fs::Permissions::from_mode(0o600))
        .map_err(|_| "evidence permissions failed")?;
    let mut bytes =
        serde_json::to_vec_pretty(value).map_err(|_| "evidence serialization failed")?;
    bytes.push(b'\n');
    file.write_all(&bytes)
        .and_then(|()| file.as_file().sync_all())
        .map_err(|_| "evidence write failed")?;
    file.persist_noclobber(path)
        .map_err(|_| "evidence destination exists/unavailable")?;
    fs::File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|_| "evidence directory sync failed".into())
}

/// Records an actually executed check against one immutable source/run identity.
/// Failed outcomes are persisted but cannot be used to create a release manifest.
pub fn accept(
    root: &Path,
    output: &Path,
    accepted: &str,
    command: &[String],
) -> Result<(), String> {
    let id = identity(root, accepted)?;
    let (program, args) = command.split_first().ok_or("check argv required")?;
    let (code, stdout, stderr) = run(program, args, root, Duration::from_secs(3600))?;
    if identity(root, accepted)? != id {
        return Err("check changed accepted source identity".into());
    }
    let mut evidence = if output.exists() {
        let evidence: Acceptance = serde_json::from_slice(&read_regular(output, 262144)?)
            .map_err(|_| "invalid evidence")?;
        if evidence.identity != id {
            return Err("evidence identity/attempt conflict".into());
        }
        evidence
    } else {
        Acceptance {
            schema_version: 1,
            scope: if id.workflow.is_some() {
                "github-actions"
            } else {
                "local"
            }
            .into(),
            identity: id,
            checks: Vec::new(),
            qualified_hosts: Vec::new(),
            payload: None,
        }
    };
    if evidence.checks.len() >= 64 {
        return Err("acceptance check count limit exceeded".into());
    }
    evidence
        .checks
        .push(check_record(command, code, &stdout, &stderr)?);
    if output.exists() {
        let temporary = output.with_extension("pending.json");
        create_json(&temporary, &evidence)?;
        fs::rename(temporary, output).map_err(|_| "evidence atomic update failed")?;
    } else {
        create_json(output, &evidence)?;
    }
    if code == 0 {
        Ok(())
    } else {
        Err("acceptance check failed; failed evidence retained".into())
    }
}

/// Checks identity and exact finite manifest inventory without trusting filenames.
pub(crate) fn validate(manifest: &Manifest) -> Result<(), String> {
    let id = manifest;
    crate::release_ops::version(&id.version)?;
    if manifest.schema_version != 1
        || id.product != "agent-run"
        || id.repository != REPOSITORY
        || id.repository_id != REPOSITORY_ID
        || id.tag != format!("v{}", id.version)
        || id.commit.len() != 40
        || !id
            .commit
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        || manifest.trust_profile != "github-attestation"
        || [&id.standard_version, &id.devkit_version, &id.baseline]
            .iter()
            .any(|value| value.is_empty() || value.len() > 128)
    {
        return Err("release identity/trust policy mismatch".into());
    }
    let w = &id.workflow;
    if w.id != WORKFLOW_ID || w.run_id == 0 || w.run_attempt == 0 || w.path != WORKFLOW_PATH {
        return Err("invalid workflow identity".into());
    }
    if manifest.artifacts.len() != 4 {
        return Err("release requires bundle/source/installer/evidence inventory".into());
    }
    let mut names = BTreeSet::new();
    let mut kinds = BTreeSet::new();
    for a in &manifest.artifacts {
        if a.name.is_empty()
            || !a
                .name
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"._-".contains(&b))
            || !names.insert(&a.name)
            || !kinds.insert(&a.kind)
            || a.size == 0
            || a.size > 512 * 1024 * 1024
            || a.sha256.len() != 64
            || !a
                .sha256
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        {
            return Err("unsafe/duplicate release inventory".into());
        }
        let fixed_name = match a.kind.as_str() {
            "bundle" => format!("agent-run-{}-aarch64-apple-darwin.tar.gz", id.version),
            "source" => format!("agent-run-{}-source.tar", id.version),
            "installer" => "install.sh".into(),
            "evidence" => ACCEPTANCE.into(),
            _ => return Err("unknown artifact kind".into()),
        };
        if a.name != fixed_name || (a.kind != "bundle" && a.target.is_some()) {
            return Err("artifact name/target layout mismatch".into());
        }
        if a.kind == "bundle"
            && (a.target.as_deref() != Some("aarch64-apple-darwin")
                || a.name != format!("agent-run-{}-aarch64-apple-darwin.tar.gz", id.version))
        {
            return Err("bundle identity mismatch".into());
        }
    }
    if kinds
        != BTreeSet::from([
            &"bundle".to_owned(),
            &"source".to_owned(),
            &"installer".to_owned(),
            &"evidence".to_owned(),
        ])
    {
        return Err("release inventory kinds mismatch".into());
    }
    Ok(())
}

/// Creates external identity from actual named files and prior observed evidence.
/// Existing directories/manifests are never overwritten; no self-hash cycle.
pub fn create(root: &Path, directory: &Path, accepted: &str) -> Result<(), String> {
    let id = identity(root, accepted)?;
    let evidence: Acceptance =
        serde_json::from_slice(&read_regular(&directory.join(ACCEPTANCE), 262144)?)
            .map_err(|_| "invalid acceptance")?;
    if evidence.identity != id
        || !evidence_ready(&evidence)
        || evidence.checks.iter().any(|c| c.exit_code != 0)
        || !evidence.qualified_hosts.is_empty()
    {
        return Err("acceptance identity/check mismatch".into());
    }
    let mut artifacts = Vec::new();
    for (name, kind) in [
        (
            format!("agent-run-{}-aarch64-apple-darwin.tar.gz", id.version),
            "bundle",
        ),
        (format!("agent-run-{}-source.tar", id.version), "source"),
        ("install.sh".into(), "installer"),
        (ACCEPTANCE.into(), "evidence"),
    ] {
        let path = directory.join(&name);
        let metadata = fs::symlink_metadata(&path).map_err(|_| "payload missing")?;
        if !metadata.is_file() {
            return Err("payload is not a regular file".into());
        }
        artifacts.push(Artifact {
            name,
            kind: kind.into(),
            target: if kind == "bundle" {
                Some(id.target.clone())
            } else {
                None
            },
            size: metadata.len(),
            sha256: digest(&path).map_err(|_| "payload hash failed")?,
        });
    }
    let manifest = Manifest::from_identity(&id, artifacts)?;
    validate(&manifest)?;
    create_json(&directory.join(MANIFEST), &manifest)?;
    let mut checksums = String::new();
    for a in &manifest.artifacts {
        checksums.push_str(&format!("{}  {}\n", a.sha256, a.name));
    }
    checksums.push_str(&format!(
        "{}  {MANIFEST}\n",
        digest(&directory.join(MANIFEST)).map_err(|_| "manifest hash failed")?
    ));
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(directory.join("SHA256SUMS"))
        .map_err(|_| "checksum file exists/unavailable")?;
    file.write_all(checksums.as_bytes())
        .map_err(|_| "checksum write failed")?;
    verify(directory, accepted, Some(&manifest.workflow)).map(|_| ())
}

/// Verifies exact hashes/sizes/check evidence and refuses extra/ambiguous assets.
/// Native extraction is inspected before the legacy internal seal is read.
pub fn verify(
    directory: &Path,
    accepted: &str,
    workflow: Option<&Workflow>,
) -> Result<Manifest, String> {
    verify_until(
        directory,
        accepted,
        workflow,
        Instant::now() + Duration::from_secs(600),
    )
}

/// Verifies the same finite inventory under an enclosing observer/publisher
/// deadline; no downloaded executable is run. Plain metadata reads are capped.
pub(crate) fn verify_until(
    directory: &Path,
    accepted: &str,
    workflow: Option<&Workflow>,
    deadline: Instant,
) -> Result<Manifest, String> {
    let bytes = read_regular(&directory.join(MANIFEST), 65536)?;
    if bytes.len() > 65536 {
        return Err("manifest byte limit exceeded".into());
    }
    let manifest: Manifest =
        serde_json::from_slice(&bytes).map_err(|_| "invalid external manifest")?;
    validate(&manifest)?;
    if manifest.commit != accepted || Some(&manifest.workflow) != workflow {
        return Err("accepted source/run/attempt mismatch".into());
    }
    let mut expected = BTreeSet::from([MANIFEST.to_owned(), "SHA256SUMS".to_owned()]);
    let checksums = String::from_utf8(read_regular(&directory.join("SHA256SUMS"), 4096)?)
        .map_err(|_| "checksums invalid UTF-8")?;
    let mut hashes = String::new();
    for a in &manifest.artifacts {
        if Instant::now() >= deadline || CANCELLED.load(std::sync::atomic::Ordering::Relaxed) {
            return Err("verification deadline/cancelled".into());
        }
        expected.insert(a.name.clone());
        let path = directory.join(&a.name);
        let metadata = fs::symlink_metadata(&path).map_err(|_| "asset missing")?;
        if !metadata.is_file()
            || metadata.len() != a.size
            || digest(&path).map_err(|_| "asset hash failed")? != a.sha256
        {
            return Err("asset size/hash mismatch".into());
        }
        hashes.push_str(&format!("{}  {}\n", a.sha256, a.name));
        if a.kind == "bundle" {
            crate::tar_guard::inspect_until(&path, true, deadline)?;
            let temporary = tempfile::tempdir().map_err(|_| "private extraction unavailable")?;
            let args = vec![
                "-xzf".into(),
                path.to_string_lossy().into_owned(),
                "-C".into(),
                temporary.path().to_string_lossy().into_owned(),
                "--no-same-owner".into(),
                "--no-same-permissions".into(),
            ];
            if run(
                "tar",
                &args,
                directory,
                deadline
                    .saturating_duration_since(Instant::now())
                    .min(Duration::from_secs(120)),
            )?
            .0 != 0
            {
                return Err("payload extraction failed".into());
            }
            crate::release::verify(temporary.path())?;
            let metadata: Value = serde_json::from_slice(&read_regular(
                &temporary.path().join("metadata.json"),
                65536,
            )?)
            .map_err(|_| "payload metadata invalid")?;
            if metadata["version"].as_str() != Some(&manifest.version) {
                return Err("internal/external version mismatch".into());
            }
        }
        if a.kind == "source" {
            crate::tar_guard::inspect_until(&path, false, deadline)?;
        }
    }
    hashes.push_str(&format!("{}  {MANIFEST}\n", digest_bytes(&bytes)));
    if hashes != checksums {
        return Err("checksum inventory mismatch".into());
    }
    let actual = fs::read_dir(directory)
        .map_err(|_| "inventory unavailable")?
        .map(|e| e.map(|e| e.file_name().to_string_lossy().into_owned()))
        .collect::<Result<BTreeSet<_>, _>>()
        .map_err(|_| "inventory unavailable")?;
    if actual != expected {
        return Err("unexpected release inventory entry".into());
    }
    let evidence: Acceptance =
        serde_json::from_slice(&read_regular(&directory.join(ACCEPTANCE), 262144)?)
            .map_err(|_| "invalid acceptance")?;
    if evidence.schema_version != 1
        || Manifest::from_identity(&evidence.identity, manifest.artifacts.clone())? != manifest
        || evidence.scope
            != if workflow.is_some() {
                "github-actions"
            } else {
                "local"
            }
        || !evidence_ready(&evidence)
        || evidence.payload.as_ref() != manifest.artifacts.iter().find(|a| a.kind == "bundle")
        || evidence.identity.target != "aarch64-apple-darwin"
        || evidence.identity.toolchain.is_empty()
        || evidence.checks.len() > 64
        || evidence.checks.iter().any(|c| {
            c.exit_code != 0
                || [&c.argv_sha256, &c.stdout_sha256, &c.stderr_sha256]
                    .iter()
                    .any(|hash| {
                        hash.len() != 64
                            || !hash
                                .bytes()
                                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
                    })
        })
        || !evidence.qualified_hosts.is_empty()
    {
        return Err("acceptance mismatch or failed check".into());
    }
    if Instant::now() >= deadline || CANCELLED.load(std::sync::atomic::Ordering::Relaxed) {
        return Err("verification deadline/cancelled".into());
    }
    Ok(manifest)
}

/// Reads a capped regular metadata descriptor without following symlinks.
/// A concurrent growth can read at most maximum+1 bytes before refusal.
pub(crate) fn read_regular(path: &Path, maximum: u64) -> Result<Vec<u8>, String> {
    let input = fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)
        .map_err(|_| "metadata file unavailable")?;
    let metadata = input
        .metadata()
        .map_err(|_| "metadata descriptor unavailable")?;
    if !metadata.is_file() || metadata.len() > maximum {
        return Err("metadata file type/size refused".into());
    }
    let mut bytes = Vec::new();
    input
        .take(maximum + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| "metadata read failed")?;
    if bytes.len() as u64 > maximum {
        return Err("metadata byte limit exceeded".into());
    }
    Ok(bytes)
}

/// Requires actual successful outcomes for all reviewed producer categories.
fn evidence_ready(evidence: &Acceptance) -> bool {
    [
        "workspace",
        "native-build",
        "source-archive",
        "integration",
        "dependency-policy",
        "exact-payload",
    ]
    .iter()
    .all(|id| {
        evidence
            .checks
            .iter()
            .any(|c| c.check_id == *id && c.exit_code == 0)
    })
}

/// Produces a fixed public check category plus a digest of exact private argv.
/// Custom argv never become command declarations or unbounded public strings.
pub(crate) fn check_record(
    command: &[String],
    code: i32,
    stdout: &[u8],
    stderr: &[u8],
) -> Result<Check, String> {
    let check_id = if command == ["cargo", "xtask", "check"] {
        "workspace"
    } else if command.starts_with(&[
        "cargo".into(),
        "xtask".into(),
        "release".into(),
        "build-native".into(),
    ]) {
        "native-build"
    } else if command.starts_with(&["cargo".into(), "xtask".into(), "archive".into()])
        && command.iter().any(|s| s == "--verify")
    {
        "source-archive"
    } else if command
        == [
            "node",
            "--test",
            "scripts/check-desktop-transport.cjs",
            "scripts/check-codegraph-probe.cjs",
        ]
    {
        "integration"
    } else if command == ["cargo", "deny", "--offline", "--locked", "check"] {
        "dependency-policy"
    } else {
        "custom"
    };
    let argv = serde_json::to_vec(command).map_err(|_| "argv digest failed")?;
    Ok(Check {
        check_id: check_id.into(),
        argv_sha256: digest_bytes(&argv),
        exit_code: code,
        stdout_sha256: digest_bytes(stdout),
        stderr_sha256: digest_bytes(stderr),
    })
}

/// Thin package/evidence command dispatch; no implicit build, tag, install or push.
pub fn command(root: &Path, args: &[String]) -> Result<(), String> {
    let field = |name: &str| {
        args.windows(2)
            .find(|p| p[0] == name)
            .map(|p| p[1].clone())
            .ok_or_else(|| format!("{name} required"))
    };
    if args.first().map(String::as_str) == Some("verify")
        && args.iter().any(|arg| arg == "--artifact")
    {
        if args.len() != 3 || args[1] != "--artifact" {
            return Err("usage: package verify --artifact ARCHIVE".into());
        }
        return crate::release_ops::verify_archive(root, Path::new(&field("--artifact")?));
    }
    let commit = field("--accepted-commit")?;
    if args.first().map(String::as_str) == Some("merge-evidence") {
        return crate::release_ops::merge_evidence(root, args, &commit);
    }
    if args.first().map(String::as_str) == Some("smoke") {
        return crate::release_ops::smoke(root, Path::new(&field("--directory")?), &commit);
    }
    match args.first().map(String::as_str) {
        Some("accept") => {
            let split = args
                .iter()
                .position(|s| s == "--")
                .ok_or("check argv separator required")?;
            accept(
                root,
                Path::new(&field("--output")?),
                &commit,
                &args[split + 1..],
            )
        }
        Some("create") => create(root, Path::new(&field("--directory")?), &commit),
        Some("verify") => {
            let dir = PathBuf::from(field("--directory")?);
            let w: Manifest = serde_json::from_slice(&read_regular(&dir.join(MANIFEST), 65536)?)
                .map_err(|_| "invalid manifest")?;
            let expected = Workflow {
                id: field("--workflow-id")?
                    .parse()
                    .map_err(|_| "invalid workflow id")?,
                path: ".github/workflows/release.yml".into(),
                run_id: field("--run-id")?.parse().map_err(|_| "invalid run id")?,
                run_attempt: field("--attempt")?.parse().map_err(|_| "invalid attempt")?,
            };
            if w.workflow != expected {
                return Err("expected workflow/run/attempt mismatch".into());
            }
            verify(&dir, &commit, Some(&expected)).map(|_| ())
        }
        _ => {
            Err("usage: cargo xtask package accept|create|verify --accepted-commit FULL_SHA".into())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Synthetic CI-shaped identity for local contract fixtures only; no live
    /// run or host qualification is asserted by these bytes.
    fn fixture_identity() -> Identity {
        Identity {
            product: "agent-run".into(),
            version: "0.19.4".into(),
            tag: "v0.19.4".into(),
            repository: REPOSITORY.into(),
            repository_id: REPOSITORY_ID,
            commit: "a".repeat(40),
            workflow: Some(Workflow {
                id: WORKFLOW_ID,
                path: ".github/workflows/release.yml".into(),
                run_id: 11,
                run_attempt: 2,
            }),
            target: "aarch64-apple-darwin".into(),
            toolchain: "1.98.1".into(),
            standard_version: "1.0.0-rc.2".into(),
            devkit_version: "0.2.0".into(),
            baseline: "rust-macos-2026-09-candidate1".into(),
        }
    }

    /// Emitted shared v1 uses exactly the pinned closed contract's required keys;
    /// non-bundle target is absent, while build proof remains in acceptance.
    #[test]
    fn shared_closed_v1_projection_and_local_nonqualification() {
        let id = fixture_identity();
        let artifacts = [
            ("bundle", "agent-run-0.19.4-aarch64-apple-darwin.tar.gz"),
            ("source", "agent-run-0.19.4-source.tar"),
            ("installer", "install.sh"),
            ("evidence", ACCEPTANCE),
        ]
        .into_iter()
        .map(|(kind, name)| Artifact {
            name: name.into(),
            kind: kind.into(),
            target: if kind == "bundle" {
                Some(id.target.clone())
            } else {
                None
            },
            size: 1,
            sha256: "b".repeat(64),
        })
        .collect();
        let manifest = Manifest::from_identity(&id, artifacts).unwrap();
        validate(&manifest).unwrap();
        let value = serde_json::to_value(&manifest).unwrap();
        // Read from pinned schemas/release-manifest.schema.json: additionalProperties=false.
        let required = BTreeSet::from([
            "schema_version",
            "repository",
            "repository_id",
            "product",
            "version",
            "tag",
            "commit",
            "workflow",
            "standard_version",
            "devkit_version",
            "baseline",
            "trust_profile",
            "artifacts",
        ]);
        assert_eq!(
            value
                .as_object()
                .unwrap()
                .keys()
                .map(String::as_str)
                .collect::<BTreeSet<_>>(),
            required
        );
        assert_eq!(
            value["workflow"]
                .as_object()
                .unwrap()
                .keys()
                .map(String::as_str)
                .collect::<BTreeSet<_>>(),
            BTreeSet::from(["id", "path", "run_id", "run_attempt"])
        );
        for artifact in value["artifacts"].as_array().unwrap() {
            assert_eq!(artifact["size"], 1);
            assert!(artifact["sha256"].as_str().unwrap().len() == 64);
            assert!(matches!(
                artifact["kind"].as_str(),
                Some("bundle" | "source" | "installer" | "evidence")
            ));
            if artifact["kind"] == "bundle" {
                assert_eq!(artifact["target"], id.target);
            } else {
                assert!(artifact.get("target").is_none());
            }
            assert!(artifact.as_object().unwrap().keys().all(|key| ["name","kind","target","size","sha256"].contains(&key.as_str())));
        }
        let mut local = id.clone();
        local.workflow = None;
        assert!(Manifest::from_identity(&local, manifest.artifacts.clone()).is_err());
        let evidence = Acceptance {
            schema_version: 1,
            identity: local,
            scope: "local".into(),
            checks: vec![],
            qualified_hosts: vec![],
            payload: None,
        };
        let value = serde_json::to_value(evidence).unwrap();
        assert!(value["identity"]["workflow"].is_null());
        assert_eq!(value["identity"]["toolchain"], "1.98.1");
        assert_eq!(value["qualified_hosts"], serde_json::json!([]));
        let mut bad = manifest;
        bad.workflow.run_attempt = 0;
        assert!(validate(&bad).is_err());
    }

    /// Fixed inventory layout and closed JSON refuse semantically different assets,
    /// metadata links and limit+1 reads before any large allocation/deserialization.
    #[test]
    fn fixed_layout_closed_fields_and_metadata_limits() {
        let id = fixture_identity();
        let artifacts = [
            ("bundle", "agent-run-0.19.4-aarch64-apple-darwin.tar.gz"),
            ("source", "agent-run-0.19.4-source.tar"),
            ("installer", "install.sh"),
            ("evidence", ACCEPTANCE),
        ]
        .into_iter()
        .map(|(kind, name)| Artifact {
            name: name.into(),
            kind: kind.into(),
            target: (kind == "bundle").then(|| id.target.clone()),
            size: 1,
            sha256: "b".repeat(64),
        })
        .collect();
        let manifest = Manifest::from_identity(&id, artifacts).unwrap();
        let mut wrong = manifest.clone();
        wrong.artifacts[1].name = "wrong-source.tar".into();
        assert!(validate(&wrong).is_err());
        let mut wrong = manifest.clone();
        wrong.artifacts[2].target = Some(id.target);
        assert!(validate(&wrong).is_err());
        let mut wrong = serde_json::to_value(&manifest).unwrap();
        wrong["unknown"] = serde_json::json!(true);
        assert!(serde_json::from_value::<Manifest>(wrong).is_err());
        let root = tempfile::tempdir().unwrap();
        let file = root.path().join("metadata");
        fs::write(&file, vec![b'x'; 65536]).unwrap();
        assert!(read_regular(&file, 65536).is_ok());
        fs::write(&file, vec![b'x'; 65537]).unwrap();
        assert!(read_regular(&file, 65536).is_err());
        std::os::unix::fs::symlink(&file, root.path().join("link")).unwrap();
        assert!(read_regular(&root.path().join("link"), 65536).is_err());
    }

    /// Secret/private argv and arbitrary output are hashed, never made public;
    /// the actual custom exit code is preserved and cannot qualify packaging.
    #[test]
    fn acceptance_drops_arbitrary_argv_and_output() {
        let argv = vec![
            "/private/SECRET_SENTINEL/tool".into(),
            "https://user:SECRET_SENTINEL@example.invalid".into(),
        ];
        let check = check_record(
            &argv,
            23,
            b"SECRET_SENTINEL stdout",
            b"SECRET_SENTINEL stderr",
        )
        .unwrap();
        let value = serde_json::to_string(&check).unwrap();
        assert!(!value.contains("SECRET_SENTINEL"));
        assert!(!value.contains("/private/"));
        assert!(!value.contains("https://"));
        assert_eq!(check.check_id, "custom");
        assert_eq!(check.exit_code, 23);
        assert_eq!(
            check.argv_sha256,
            digest_bytes(&serde_json::to_vec(&argv).unwrap())
        );
        let evidence = Acceptance {
            schema_version: 1,
            identity: fixture_identity(),
            scope: "github-actions".into(),
            checks: vec![check],
            qualified_hosts: vec![],
            payload: None,
        };
        assert!(!evidence_ready(&evidence));
    }

    /// The direct leader exits first while a finite recorded descendant retains
    /// stdout; deadline cleanup handles that descendant and joins both drains.
    #[test]
    fn exited_leader_pipe_descendant_cleanup_is_bounded() {
        let root = tempfile::tempdir().unwrap();
        let pid_file = root.path().join("descendant");
        let args = vec![
            "-c".into(),
            "sleep 20 & echo $! > \"$1\"; sleep 0.2; exit 0".into(),
            "fixture".into(),
            pid_file.to_string_lossy().into_owned(),
        ];
        let start = Instant::now();
        assert!(run("/bin/sh", &args, root.path(), Duration::from_millis(700)).is_err());
        assert!(start.elapsed() < Duration::from_secs(8));
        let pid: i32 = fs::read_to_string(pid_file)
            .unwrap()
            .trim()
            .parse()
            .unwrap();
        assert!(
            agent_run_platform::process::inspect(pid)
                .map(|p| p.zombie)
                .unwrap_or(true),
            "captured descendant must be dead"
        );
    }

    /// A same-version native release can be a no-op only for identical bytes.
    #[test]
    fn same_version_binary_conflict_preserves_existing_bytes() {
        let root = tempfile::tempdir().unwrap();
        let binary = root.path().join("binary");
        fs::write(&binary, b"one").unwrap();
        let release = crate::release::build(root.path(), "0.19.4", &binary).unwrap();
        fs::write(&binary, b"two").unwrap();
        assert!(
            crate::release::build(root.path(), "0.19.4", &binary)
                .unwrap_err()
                .contains("conflict")
        );
        assert_eq!(fs::read(release.join("bin/agent-run")).unwrap(), b"one");
    }
}
