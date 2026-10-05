//! Foreground exact-release observer. No publication, installation or host wake.
use crate::delivery::{
    self, MANIFEST, Manifest, REPOSITORY, REPOSITORY_ID, WORKFLOW_ID, WORKFLOW_PATH, Workflow,
};
use serde_json::{Value, json};
use std::{
    collections::BTreeSet,
    fs,
    path::{Path, PathBuf},
    sync::atomic::Ordering,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

/// Typed terminal observer error: stable exit code and bounded public reason.
#[derive(Debug)]
pub(crate) struct Failure {
    /// Exit code from the documented observer contract.
    pub code: i32,
    /// Fixed diagnostic, never remote response or credential text.
    pub reason: String,
}
/// Constructs a fixed terminal failure without propagating remote/private output.
fn failure(code: i32, reason: &str) -> Failure {
    Failure {
        code,
        reason: reason.into(),
    }
}

/// Exact requested identity and finite invocation limits.
struct Options {
    /// Stable version tag, validated before constructing API paths.
    tag: String,
    /// Full lowercase accepted source SHA.
    commit: String,
    /// Optional exact run/attempt, or bounded unambiguous discovery.
    workflow: Option<Workflow>,
    /// Create-new absolute private result file.
    result: PathBuf,
    /// Monotonic total deadline duration.
    timeout: Duration,
    /// Capped polling interval, including queue time.
    interval: Duration,
    /// Trusted absolute notifier executable and literal argv.
    notify: Vec<String>,
}
impl Options {
    /// Rejects unknown/duplicate identity arguments and malformed finite limits.
    fn parse(args: &[String]) -> Result<Self, Failure> {
        let mut values = std::collections::BTreeMap::new();
        let mut notify_args = Vec::new();
        for pair in args.chunks(2) {
            if pair.len() != 2 {
                return Err(failure(3, "option value missing"));
            }
            if pair[0] == "--notify-arg" {
                notify_args.push(pair[1].clone());
                continue;
            }
            if ![
                "--repo",
                "--tag",
                "--commit",
                "--run-id",
                "--attempt",
                "--workflow",
                "--result-file",
                "--timeout",
                "--interval",
                "--notify-exec",
            ]
            .contains(&pair[0].as_str())
                || values.insert(pair[0].clone(), pair[1].clone()).is_some()
            {
                return Err(failure(3, "unknown/duplicate option"));
            }
        }
        let required = |key: &str| {
            values
                .get(key)
                .cloned()
                .ok_or_else(|| failure(3, "required identity/result option missing"))
        };
        if required("--repo")? != REPOSITORY {
            return Err(failure(3, "official repository required"));
        }
        let tag = required("--tag")?;
        crate::release_ops::version(
            tag.strip_prefix('v')
                .ok_or_else(|| failure(3, "stable vX.Y.Z tag required"))?,
        )
        .map_err(|_| failure(3, "stable vX.Y.Z tag required"))?;
        let commit = required("--commit")?;
        if commit.len() != 40
            || !commit
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        {
            return Err(failure(3, "full lowercase accepted SHA required"));
        }
        if values
            .get("--workflow")
            .is_some_and(|s| s != "release.yml" && s != WORKFLOW_PATH)
        {
            return Err(failure(3, "reviewed release workflow required"));
        }
        let number = |key: &str, default: u64, max: u64| -> Result<u64, Failure> {
            let value = values
                .get(key)
                .map(|s| s.parse::<u64>())
                .transpose()
                .map_err(|_| failure(3, "invalid numeric limit/identity"))?
                .unwrap_or(default);
            if value == 0 || value > max {
                return Err(failure(3, "numeric limit/identity out of range"));
            }
            Ok(value)
        };
        let workflow = if values.contains_key("--run-id") {
            Some(Workflow {
                id: WORKFLOW_ID,
                path: WORKFLOW_PATH.into(),
                run_id: number("--run-id", 0, u64::MAX)?,
                run_attempt: number("--attempt", 0, u64::MAX)?,
            })
        } else {
            if values.contains_key("--attempt") {
                return Err(failure(3, "attempt requires run-id"));
            }
            None
        };
        let result = PathBuf::from(required("--result-file")?);
        if !result.is_absolute() || fs::symlink_metadata(&result).is_ok() {
            return Err(failure(5, "result must be absolute and create-new"));
        }
        let mut notify = Vec::new();
        if let Some(program) = values.get("--notify-exec") {
            if !Path::new(program).is_absolute() {
                return Err(failure(3, "notifier must be a trusted absolute executable"));
            }
            notify.push(program.clone());
            notify.extend(notify_args);
        } else if !notify_args.is_empty() {
            return Err(failure(3, "notify-arg requires notify-exec"));
        }
        if notify.len() > 33 || notify.iter().map(String::len).sum::<usize>() > 16384 {
            return Err(failure(3, "notifier argv limit exceeded"));
        }
        Ok(Self {
            tag,
            commit,
            workflow,
            result,
            timeout: Duration::from_secs(number("--timeout", 1800, 86400)?),
            interval: Duration::from_secs(number("--interval", 15, 300)?),
            notify,
        })
    }
}

/// Observes SIGINT/TERM using existing libc only, restoring prior handlers on exit.
struct Signals {
    /// Original SIGINT handler.
    interrupt: libc::sighandler_t,
    /// Original SIGTERM handler.
    terminate: libc::sighandler_t,
}
/// Async-signal-safe best-effort cancellation marker; no I/O in a signal handler.
extern "C" fn cancelled(_: libc::c_int) {
    delivery::CANCELLED.store(true, Ordering::Relaxed);
}
impl Signals {
    /// Installs handlers for this foreground invocation only.
    fn install() -> Self {
        delivery::CANCELLED.store(false, Ordering::Relaxed);
        // SAFETY: static handler only performs a lock-free atomic store.
        unsafe {
            Self {
                interrupt: libc::signal(libc::SIGINT, cancelled as *const () as libc::sighandler_t),
                terminate: libc::signal(
                    libc::SIGTERM,
                    cancelled as *const () as libc::sighandler_t,
                ),
            }
        }
    }
}
impl Drop for Signals {
    /// Restores previous host signal behavior after owned subprocesses are reaped.
    fn drop(&mut self) {
        // SAFETY: these are the exact prior handlers returned by signal at install.
        unsafe {
            libc::signal(libc::SIGINT, self.interrupt);
            libc::signal(libc::SIGTERM, self.terminate);
        }
        delivery::CANCELLED.store(false, Ordering::Relaxed);
    }
}

/// Computes each operation budget from the same monotonic deadline.
pub(crate) fn budget(deadline: Instant, cap: u64) -> Result<Duration, Failure> {
    if delivery::CANCELLED.load(Ordering::Relaxed) {
        return Err(failure(130, "cancelled"));
    }
    let remaining = deadline
        .checked_duration_since(Instant::now())
        .ok_or_else(|| failure(2, "deadline exceeded"))?;
    Ok(remaining.min(Duration::from_secs(cap)))
}

/// Borrowed bounded HTTP envelope from gh's documented --include output.
struct ApiResponse<'a> {
    /// Actual HTTP status, before interpreting a JSON body.
    status: u16,
    /// Optional server delay; only interpreted for a retryable response.
    retry_after: Option<&'a str>,
    /// Primary rate-limit reset in UTC epoch seconds.
    rate_reset: Option<&'a str>,
    /// True only for an explicit zero x-ratelimit-remaining header.
    rate_limited: bool,
    /// Body remains bounded by the existing owned subprocess reader.
    body: &'a [u8],
}

/// Splits LF/CRLF envelopes within 16 KiB/128 lines; rejects malformed/duplicate
/// retry headers. No body/header bytes become diagnostics or public evidence.
fn api_response(bytes: &[u8]) -> Result<ApiResponse<'_>, Failure> {
    let (end, delimiter) = bytes
        .windows(4)
        .position(|s| s == b"\r\n\r\n")
        .map(|i| (i, 4))
        .into_iter()
        .chain(bytes.windows(2).position(|s| s == b"\n\n").map(|i| (i, 2)))
        .min_by_key(|(offset, _)| *offset)
        .ok_or_else(|| failure(5, "GitHub HTTP header delimiter missing"))?;
    if end > 16384 {
        return Err(failure(5, "GitHub HTTP header byte limit exceeded"));
    }
    let head = std::str::from_utf8(&bytes[..end])
        .map_err(|_| failure(5, "GitHub HTTP headers invalid"))?;
    let mut lines = head.lines();
    let status_line = lines
        .next()
        .ok_or_else(|| failure(5, "GitHub HTTP status missing"))?;
    if status_line.len() > 2048 {
        return Err(failure(5, "GitHub HTTP status line limit exceeded"));
    }
    let mut status = status_line.split_whitespace();
    if ![
        "HTTP/1.0", "HTTP/1.1", "HTTP/2", "HTTP/2.0", "HTTP/3", "HTTP/3.0",
    ]
    .contains(&status.next().unwrap_or_default())
    {
        return Err(failure(5, "GitHub HTTP version invalid"));
    }
    let status: u16 = status
        .next()
        .ok_or_else(|| failure(5, "GitHub HTTP status invalid"))?
        .parse()
        .map_err(|_| failure(5, "GitHub HTTP status invalid"))?;
    if !(100..600).contains(&status) {
        return Err(failure(5, "GitHub HTTP status invalid"));
    }
    let mut response = ApiResponse {
        status,
        retry_after: None,
        rate_reset: None,
        rate_limited: false,
        body: &bytes[end + delimiter..],
    };
    let mut remaining = None;
    for (index, line) in lines.enumerate() {
        if index >= 127 || line.len() > 2048 {
            return Err(failure(5, "GitHub HTTP header count/line limit exceeded"));
        }
        let (name, value) = line
            .split_once(':')
            .ok_or_else(|| failure(5, "GitHub HTTP header invalid"))?;
        let value = value.trim();
        let slot = if name.eq_ignore_ascii_case("retry-after") {
            Some(&mut response.retry_after)
        } else if name.eq_ignore_ascii_case("x-ratelimit-reset") {
            Some(&mut response.rate_reset)
        } else if name.eq_ignore_ascii_case("x-ratelimit-remaining") {
            Some(&mut remaining)
        } else {
            None
        };
        if let Some(slot) = slot
            && slot.replace(value).is_some()
        {
            return Err(failure(5, "GitHub HTTP retry header duplicated"));
        }
    }
    if let Some(remaining) = remaining {
        response.rate_limited = remaining
            .parse::<u64>()
            .map_err(|_| failure(5, "GitHub rate-limit remaining invalid"))?
            == 0;
    }
    Ok(response)
}

/// Converts delta seconds, HTTP-date Retry-After, or UTC reset seconds into a
/// maximum 60-second pause. Larger/invalid values fail rather than retry early.
fn retry_delay(value: &str, epoch: bool) -> Result<Duration, Failure> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| failure(4, "UTC retry clock unavailable"))?
        .as_secs();
    let seconds = if let Ok(number) = value.parse::<u64>() {
        if epoch {
            number.saturating_sub(now)
        } else {
            number
        }
    } else if !epoch {
        let value =
            std::ffi::CString::new(value).map_err(|_| failure(5, "GitHub retry date invalid"))?;
        // SAFETY: tm is initialized; strptime reads live NUL-terminated inputs,
        // returned tail stays inside value, and timegm only updates this local tm.
        unsafe {
            let mut time: libc::tm = std::mem::zeroed();
            let tail = libc::strptime(
                value.as_ptr(),
                c"%a, %d %b %Y %H:%M:%S GMT".as_ptr(),
                &mut time,
            );
            if tail.is_null() || *tail != 0 {
                return Err(failure(5, "GitHub retry date invalid"));
            }
            let date = (time.tm_year, time.tm_mon, time.tm_mday);
            let epoch = libc::timegm(&mut time);
            if epoch < 0 || date != (time.tm_year, time.tm_mon, time.tm_mday) {
                return Err(failure(5, "GitHub retry date invalid"));
            }
            (epoch as u64).saturating_sub(now)
        }
    } else {
        return Err(failure(5, "GitHub rate-limit reset invalid"));
    };
    if seconds > 60 {
        return Err(failure(4, "GitHub retry delay exceeds bounded policy"));
    }
    Ok(Duration::from_secs(seconds))
}

/// Sleeps in short cancellation-aware slices without extending the caller's
/// monotonic deadline; an insufficient remaining budget returns deadline exit 2.
fn retry_pause(deadline: Instant, delay: Duration) -> Result<(), Failure> {
    let until = Instant::now() + delay;
    loop {
        let remaining = budget(deadline, 60)?;
        let pause = until.saturating_duration_since(Instant::now());
        if pause.is_zero() {
            return Ok(());
        }
        std::thread::sleep(pause.min(remaining).min(Duration::from_millis(50)));
    }
}

/// Performs explicit GET reads with at most three owned attempts under the same
/// monotonic deadline. Only allowed 404 means waiting after successful preflight.
/// Auth/ordinary denial and malformed metadata fail immediately; network, 429,
/// rate-limit 403 and 5xx use bounded Retry-After/reset or exponential backoff.
pub(crate) fn api(
    root: &Path,
    path: &str,
    deadline: Instant,
    missing: bool,
) -> Result<Option<Value>, Failure> {
    let args = vec![
        "api".into(),
        "--hostname".into(),
        "github.com".into(),
        path.into(),
        "--include".into(),
        "--method".into(),
        "GET".into(),
    ];
    for attempt in 0..3 {
        let response = delivery::run("gh", &args, root, budget(deadline, 30)?);
        budget(deadline, 30)?;
        let mut delay = Duration::from_millis(250 << attempt);
        match response {
            Err(error) if error == "external tool unavailable" => {
                return Err(failure(4, "GitHub CLI unavailable"));
            }
            Err(error) if error == "external tool output limit/read failure" => {
                return Err(failure(5, "GitHub response byte/read limit failure"));
            }
            Err(_) => {}
            Ok((4, _, _)) => return Err(failure(4, "GitHub authentication required")),
            Ok((code, out, _)) => {
                if !out.starts_with(b"HTTP/") {
                    if code == 0 {
                        return Err(failure(5, "GitHub HTTP status/headers missing"));
                    }
                } else {
                    let response = api_response(&out)?;
                    if response.status == 404 {
                        return if missing {
                            Ok(None)
                        } else {
                            Err(failure(4, "GitHub object unavailable or access denied"))
                        };
                    }
                    if response.status == 401
                        || (response.status == 403
                            && response.retry_after.is_none()
                            && !response.rate_limited)
                    {
                        return Err(failure(4, "GitHub authentication/access denied"));
                    }
                    if (200..300).contains(&response.status) {
                        if code != 0 {
                            return Err(failure(
                                4,
                                "GitHub CLI failed for a successful HTTP response",
                            ));
                        }
                        return serde_json::from_slice(response.body)
                            .map(Some)
                            .map_err(|_| failure(5, "GitHub JSON response invalid"));
                    }
                    if response.status == 429
                        || response.status == 403
                        || (500..600).contains(&response.status)
                    {
                        if let Some(value) = response.retry_after {
                            delay = delay.max(retry_delay(value, false)?);
                        } else if response.rate_limited {
                            delay = delay.max(retry_delay(
                                response.rate_reset.ok_or_else(|| {
                                    failure(4, "GitHub rate-limit reset unavailable")
                                })?,
                                true,
                            )?);
                        } else if response.status == 429 || response.status == 403 {
                            delay = Duration::from_secs(60);
                        }
                    } else {
                        return Err(failure(4, "GitHub HTTP request rejected"));
                    }
                }
            }
        }
        if attempt == 2 {
            return Err(failure(4, "GitHub transient retries exhausted"));
        }
        eprintln!("Retrying GitHub read ({}/3)", attempt + 2);
        retry_pause(deadline, delay)?;
    }
    Err(failure(4, "GitHub transient retries exhausted"))
}

/// Requires the numeric repository/workflow identity before a 404 may mean wait.
pub(crate) fn preflight(root: &Path, deadline: Instant) -> Result<(), Failure> {
    let repo = api(root, &format!("repos/{REPOSITORY}"), deadline, false)?
        .ok_or_else(|| failure(4, "repository unavailable"))?;
    let workflow = api(
        root,
        &format!("repos/{REPOSITORY}/actions/workflows/{WORKFLOW_ID}"),
        deadline,
        false,
    )?
    .ok_or_else(|| failure(4, "workflow unavailable"))?;
    if repo["id"] != REPOSITORY_ID
        || repo["full_name"] != REPOSITORY
        || workflow["id"] != WORKFLOW_ID
        || workflow["path"] != WORKFLOW_PATH
        || workflow["state"] != "active"
    {
        return Err(failure(5, "repository/workflow identity changed"));
    }
    Ok(())
}

/// Resolves an annotated tag with bounded depth; returns immutable ref-object SHA
/// so replacing a tag object while keeping its commit is also detected.
pub(crate) fn tag(
    root: &Path,
    name: &str,
    commit: &str,
    deadline: Instant,
) -> Result<Option<String>, Failure> {
    let Some(reference) = api(
        root,
        &format!("repos/{REPOSITORY}/git/ref/tags/{name}"),
        deadline,
        true,
    )?
    else {
        return Ok(None);
    };
    if reference["ref"] != format!("refs/tags/{name}") || reference["object"]["type"] != "tag" {
        return Err(failure(5, "annotated exact tag required"));
    }
    let original = reference["object"]["sha"]
        .as_str()
        .ok_or_else(|| failure(5, "tag object identity missing"))?
        .to_owned();
    let mut sha = original.clone();
    for _ in 0..5 {
        let value = api(
            root,
            &format!("repos/{REPOSITORY}/git/tags/{sha}"),
            deadline,
            false,
        )?
        .ok_or_else(|| failure(5, "annotated tag absent"))?;
        if value["sha"] != sha {
            return Err(failure(5, "tag object identity mismatch"));
        }
        match value["object"]["type"].as_str() {
            Some("commit") if value["object"]["sha"] == commit => return Ok(Some(original)),
            Some("tag") => {
                sha = value["object"]["sha"]
                    .as_str()
                    .ok_or_else(|| failure(5, "nested tag identity absent"))?
                    .into()
            }
            _ => return Err(failure(5, "tag moved/commit mismatch")),
        }
    }
    Err(failure(5, "tag depth exceeded"))
}

/// Requires exact push run, branch/tag, repository, path, SHA and attempt identity.
pub(crate) fn validate_run(
    value: &Value,
    w: &Workflow,
    tag: &str,
    commit: &str,
) -> Result<bool, Failure> {
    if value["id"] != w.run_id
        || value["run_attempt"] != w.run_attempt
        || value["workflow_id"] != WORKFLOW_ID
        || value["path"] != WORKFLOW_PATH
        || value["head_sha"] != commit
        || value["head_branch"] != tag
        || value["event"] != "push"
        || value["repository"]["id"] != REPOSITORY_ID
        || value["repository"]["full_name"] != REPOSITORY
    {
        return Err(failure(5, "workflow run/attempt/source identity mismatch"));
    }
    if value["status"] != "completed" {
        return Ok(false);
    }
    if value["conclusion"] != "success" {
        return Err(failure(1, "exact workflow failed/cancelled"));
    }
    Ok(true)
}

/// Discovers at most five pages; multiple exact candidates fail closed, never latest.
fn discover(root: &Path, o: &Options, deadline: Instant) -> Result<Option<Workflow>, Failure> {
    let mut candidates = Vec::new();
    for page in 1..=5 {
        let response=api(root,&format!("repos/{REPOSITORY}/actions/workflows/{WORKFLOW_ID}/runs?event=push&head_sha={}&per_page=100&page={page}",o.commit),deadline,false)?.ok_or_else(||failure(4,"runs unavailable"))?;
        let runs = response["workflow_runs"]
            .as_array()
            .ok_or_else(|| failure(5, "runs response invalid"))?;
        for value in runs {
            if value["head_sha"] == o.commit
                && value["head_branch"] == o.tag
                && value["event"] == "push"
            {
                let w = Workflow {
                    id: WORKFLOW_ID,
                    path: WORKFLOW_PATH.into(),
                    run_id: value["id"]
                        .as_u64()
                        .ok_or_else(|| failure(5, "run id invalid"))?,
                    run_attempt: value["run_attempt"]
                        .as_u64()
                        .ok_or_else(|| failure(5, "run attempt invalid"))?,
                };
                validate_run(value, &w, &o.tag, &o.commit)?;
                candidates.push(w);
            }
        }
        if runs.len() < 100 {
            break;
        }
        if page == 5 {
            return Err(failure(5, "run pagination limit reached"));
        }
    }
    if candidates.len() > 1 {
        return Err(failure(
            5,
            "ambiguous runs; explicit run-id/attempt required",
        ));
    }
    Ok(candidates.pop())
}

/// Confirms the exact published stable release and complete finite asset inventory.
pub(crate) fn release_identity(value: &Value, tag: &str, published: bool) -> Result<(), Failure> {
    if value["tag_name"] != tag
        || value["prerelease"] != false
        || value["draft"] != !published
        || (published && value["published_at"].as_str().is_none())
        || !value["id"].as_u64().is_some_and(|id| id > 0)
    {
        return Err(failure(5, "release publication/stable identity mismatch"));
    }
    Ok(())
}

/// Checks API inventory independently before downloading; every file is capped.
pub(crate) fn inventory(
    value: &Value,
    manifest: &Manifest,
    manifest_size: u64,
) -> Result<(), Failure> {
    let assets = value["assets"]
        .as_array()
        .ok_or_else(|| failure(5, "release assets invalid"))?;
    let mut expected = manifest
        .artifacts
        .iter()
        .map(|a| (a.name.as_str(), a.size))
        .collect::<std::collections::BTreeMap<_, _>>();
    expected.insert(MANIFEST, manifest_size);
    let mut seen = BTreeSet::new();
    for asset in assets {
        let name = asset["name"]
            .as_str()
            .ok_or_else(|| failure(5, "asset name invalid"))?;
        let size = asset["size"]
            .as_u64()
            .ok_or_else(|| failure(5, "asset size invalid"))?;
        if !seen.insert(name)
            || asset["state"] != "uploaded"
            || size == 0
            || size > 512 * 1024 * 1024
            || (name == "SHA256SUMS" && size > 4096)
            || (name != "SHA256SUMS" && expected.get(name) != Some(&size))
        {
            return Err(failure(5, "release asset inventory/size conflict"));
        }
    }
    if assets.len() != expected.len() + 1
        || !seen.contains("SHA256SUMS")
        || expected.keys().any(|name| !seen.contains(name))
    {
        return Err(failure(5, "release inventory incomplete/extra"));
    }
    Ok(())
}

/// Downloads only the six exact approved names; create-new scratch plus child
/// file-size limit prevents overwrite/unbounded payloads. Partial downloads remain.
pub(crate) fn download(
    root: &Path,
    tag: &str,
    directory: &Path,
    deadline: Instant,
    manifest_only: bool,
) -> Result<(), Failure> {
    let mut args = vec![
        "release".into(),
        "download".into(),
        tag.into(),
        "--repo".into(),
        REPOSITORY.into(),
        "--dir".into(),
        directory.to_string_lossy().into_owned(),
    ];
    if manifest_only {
        args.extend(["--pattern".into(), MANIFEST.into()]);
    } else {
        for name in [
            format!(
                "agent-run-{}-aarch64-apple-darwin.tar.gz",
                tag.trim_start_matches('v')
            ),
            format!("agent-run-{}-source.tar", tag.trim_start_matches('v')),
            "install.sh".into(),
            "acceptance.json".into(),
            "SHA256SUMS".into(),
        ] {
            args.extend(["--pattern".into(), name]);
        }
    }
    let outcome = delivery::run("gh", &args, root, budget(deadline, 180)?);
    budget(deadline, 180)?;
    let (code, _, _) = outcome.map_err(|_| failure(4, "bounded release download failed"))?;
    if code != 0 {
        return Err(failure(
            4,
            "release download failed; partial scratch retained",
        ));
    }
    Ok(())
}

/// Reads reviewed TOML declarations at the exact source SHA through GitHub raw
/// contents; refuses unbounded/invalid source and binds family/version/toolchain.
fn source_contract(root: &Path, manifest: &Manifest, deadline: Instant) -> Result<String, Failure> {
    let mut declarations = Vec::new();
    for name in ["Cargo.toml", "family.toml", "rust-toolchain.toml"] {
        let args = vec![
            "api".into(),
            "--hostname".into(),
            "github.com".into(),
            "-H".into(),
            "Accept: application/vnd.github.raw+json".into(),
            format!("repos/{REPOSITORY}/contents/{name}?ref={}", manifest.commit),
        ];
        let outcome = delivery::run("gh", &args, root, budget(deadline, 30)?);
        budget(deadline, 30)?;
        let (code, bytes, _) = outcome.map_err(|_| failure(4, "source declaration unavailable"))?;
        if code != 0 || bytes.len() > 65536 {
            return Err(failure(5, "source declaration type/size invalid"));
        }
        declarations.push(
            serde_json::to_value(
                toml::from_str::<toml::Value>(
                    std::str::from_utf8(&bytes)
                        .map_err(|_| failure(5, "source declaration invalid UTF-8"))?,
                )
                .map_err(|_| failure(5, "source declaration invalid TOML"))?,
            )
            .map_err(|_| failure(5, "source declaration invalid value"))?,
        );
    }
    if declarations[0]["workspace"]["package"]["version"].as_str() != Some(&manifest.version)
        || declarations[1]["standard_version"].as_str() != Some(&manifest.standard_version)
        || declarations[1]["devkit_version"].as_str() != Some(&manifest.devkit_version)
        || declarations[1]["baseline"].as_str() != Some(&manifest.baseline)
        || declarations[1]["release"]["trust_profile"].as_str() != Some(&manifest.trust_profile)
    {
        return Err(failure(
            5,
            "accepted-source declaration conflicts with manifest",
        ));
    }
    let pinned = declarations[2]["toolchain"]["channel"]
        .as_str()
        .ok_or_else(|| failure(5, "source compiler declaration missing"))?;
    Ok(pinned.to_owned())
}

/// Produces deterministic identity/outcome event IDs, independent of notification.
fn event(
    o: &Options,
    w: Option<&Workflow>,
    status: &str,
    reason: &str,
    manifest_hash: Option<&str>,
) -> Value {
    let identity = json!([
        REPOSITORY,
        REPOSITORY_ID,
        o.tag,
        o.commit,
        w,
        status,
        reason,
        manifest_hash
    ]);
    json!({"schema_version":1,"event_id":format!("sha256:{}",crate::release::digest_bytes(&serde_json::to_vec(&identity).unwrap_or_default())),"event":if status=="released"{"release.ready"}else{"release.finished"},"status":status,"reason":reason,"repository":REPOSITORY,"repository_id":REPOSITORY_ID,"tag":o.tag,"commit":o.commit,"run_id":w.map(|w|w.run_id),"run_attempt":w.map(|w|w.run_attempt),"release_url":format!("https://github.com/{REPOSITORY}/releases/tag/{}",o.tag),"manifest_sha256":manifest_hash,"artifact_integrity":if status=="released"{"verified"}else{"not_verified"},"provenance_verification":"not_performed","installed":false,"agent_awakened":false,"notification":{"status":if o.notify.is_empty(){"not_requested"}else{"pending"}}})
}

/// Runs read-only polling to exact success, keeping all waits inside one deadline.
fn observe(root: &Path, o: &Options, w: &mut Option<Workflow>) -> Result<String, Failure> {
    let deadline = Instant::now() + o.timeout;
    preflight(root, deadline)?;
    let mut original_tag = None;
    loop {
        budget(deadline, 30)?;
        if let Some(current) = tag(root, &o.tag, &o.commit, deadline)? {
            if original_tag.as_ref().is_some_and(|old| old != &current) {
                return Err(failure(5, "tag object changed during wait"));
            }
            original_tag = Some(current);
            if w.is_none() {
                *w = discover(root, o, deadline)?;
            }
            if let Some(workflow) = w.as_ref() {
                let run = api(
                    root,
                    &format!("repos/{REPOSITORY}/actions/runs/{}", workflow.run_id),
                    deadline,
                    false,
                )?
                .ok_or_else(|| failure(5, "run disappeared"))?;
                if validate_run(&run, workflow, &o.tag, &o.commit)?
                    && let Some(release) = api(
                        root,
                        &format!("repos/{REPOSITORY}/releases/tags/{}", o.tag),
                        deadline,
                        true,
                    )?
                    && release["draft"] == false
                {
                    release_identity(&release, &o.tag, true)?;
                    fs::create_dir_all(root.join("target/release-observer"))
                        .map_err(|_| failure(5, "scratch root unavailable"))?;
                    let scratch = tempfile::Builder::new()
                        .prefix("wait-")
                        .tempdir_in(root.join("target/release-observer"))
                        .map_err(|_| failure(5, "scratch unavailable"))?
                        .keep();
                    download(root, &o.tag, &scratch, deadline, true)?;
                    let bytes = delivery::read_regular(&scratch.join(MANIFEST), 65536)
                        .map_err(|_| failure(5, "manifest download size/type refused"))?;
                    if bytes.len() > 65536 {
                        return Err(failure(5, "manifest size limit"));
                    }
                    let manifest: Manifest = serde_json::from_slice(&bytes)
                        .map_err(|_| failure(5, "manifest invalid"))?;
                    delivery::validate(&manifest)
                        .map_err(|_| failure(5, "manifest policy invalid"))?;
                    if manifest.tag != o.tag
                        || manifest.commit != o.commit
                        || &manifest.workflow != workflow
                    {
                        return Err(failure(5, "published manifest exact identity mismatch"));
                    }
                    inventory(&release, &manifest, bytes.len() as u64)?;
                    let pinned_toolchain = source_contract(root, &manifest, deadline)?;
                    download(root, &o.tag, &scratch, deadline, false)?;
                    let verified =
                        delivery::verify_until(&scratch, &o.commit, Some(workflow), deadline);
                    budget(deadline, 30)?;
                    verified.map_err(|_| failure(5, "artifact/evidence integrity failure"))?;
                    let evidence: delivery::Acceptance = serde_json::from_slice(
                        &delivery::read_regular(&scratch.join(delivery::ACCEPTANCE), 262144)
                            .map_err(|_| failure(5, "acceptance metadata type/size invalid"))?,
                    )
                    .map_err(|_| failure(5, "acceptance invalid"))?;
                    if evidence.identity.toolchain != pinned_toolchain {
                        return Err(failure(5, "accepted-source compiler/evidence conflict"));
                    }
                    budget(deadline, 30)?;
                    preflight(root, deadline)?;
                    if tag(root, &o.tag, &o.commit, deadline)? != original_tag {
                        return Err(failure(5, "final tag identity changed"));
                    }
                    let final_run = api(
                        root,
                        &format!("repos/{REPOSITORY}/actions/runs/{}", workflow.run_id),
                        deadline,
                        false,
                    )?
                    .ok_or_else(|| failure(5, "final run absent"))?;
                    if !validate_run(&final_run, workflow, &o.tag, &o.commit)? {
                        return Err(failure(5, "final run state changed"));
                    }
                    let final_release = api(
                        root,
                        &format!("repos/{REPOSITORY}/releases/tags/{}", o.tag),
                        deadline,
                        false,
                    )?
                    .ok_or_else(|| failure(5, "final release absent"))?;
                    release_identity(&final_release, &o.tag, true)?;
                    if final_release["id"] != release["id"]
                        || final_release["assets"] != release["assets"]
                        || final_release["published_at"] != release["published_at"]
                    {
                        return Err(failure(5, "final release identity/inventory changed"));
                    }
                    return Ok(crate::release::digest_bytes(&bytes));
                }
            }
        } else if original_tag.is_some() {
            return Err(failure(5, "tag disappeared during wait"));
        }
        eprintln!("Waiting for exact release {}", o.tag);
        let until = Instant::now() + o.interval.min(budget(deadline, 300)?);
        while Instant::now() < until {
            budget(deadline, 1)?;
            std::thread::sleep(Duration::from_millis(100));
        }
    }
}

/// Records terminal JSON before a finite notifier receives it on stdin. Exact
/// event-id ACK means adapter receipt only; it never claims an agent was awakened.
pub fn command(root: &Path, args: &[String]) -> i32 {
    let o = match Options::parse(args) {
        Ok(o) => o,
        Err(error) => {
            println!(
                "{}",
                json!({"schema_version":1,"status":"failed","reason":error.reason,"installed":false,"agent_awakened":false})
            );
            return error.code;
        }
    };
    let _signals = Signals::install();
    let mut workflow = o.workflow.clone();
    let (status, reason, hash, mut code) = match observe(root, &o, &mut workflow) {
        Ok(hash) => (
            "released",
            "exact release verified".to_owned(),
            Some(hash),
            0,
        ),
        Err(error) => (
            if error.code == 130 {
                "cancelled"
            } else if error.code == 2 {
                "timeout"
            } else {
                "failed"
            },
            error.reason,
            None,
            error.code,
        ),
    };
    let mut result = event(&o, workflow.as_ref(), status, &reason, hash.as_deref());
    if delivery::create_json(&o.result, &result).is_err() {
        result["reason"] = json!("create-new durable result failed");
        println!("{result}");
        return 5;
    }
    if !o.notify.is_empty() {
        delivery::CANCELLED.store(false, Ordering::Relaxed);
        let ack = delivery::run_io(
            &o.notify[0],
            &o.notify[1..],
            root,
            Duration::from_secs(15),
            Some(&o.result),
        )
        .ok()
        .filter(|(code, _, _)| *code == 0)
        .and_then(|(_, out, _)| serde_json::from_slice::<Value>(&out).ok());
        let accepted = ack.as_ref().is_some_and(|a| {
            a.as_object().is_some_and(|m| m.len() == 2)
                && a["event_id"] == result["event_id"]
                && a["status"] == "accepted"
        });
        result["notification"] = json!({"status":if accepted{"acknowledged"}else{"failed"}});
        let pending = o.result.with_extension("notification.pending.json");
        if delivery::create_json(&pending, &result)
            .and_then(|()| {
                fs::rename(&pending, &o.result)
                    .map_err(|_| "notification result update failed".into())
            })
            .is_err()
        {
            code = 5;
        } else if !accepted {
            code = 6;
        }
    }
    println!("{result}");
    code
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Header delimiters, duplicates, limits and retry timing fail conservatively;
    /// the JSON body cannot be mistaken for a later CRLF header terminator.
    #[test]
    fn http_envelope_and_retry_timing_boundaries() {
        let response = api_response(b"HTTP/2.0 200 OK\nX-Test: value\n\n{}\r\n\r\n").unwrap();
        assert_eq!(response.status, 200);
        assert_eq!(response.body, b"{}\r\n\r\n");
        assert!(
            api_response(b"HTTP/2.0 429 Retry\r\nRetry-After: 1\r\nretry-after: 2\r\n\r\n{}")
                .is_err()
        );
        let oversized = format!("HTTP/2.0 200 OK\nX-Test: {}\n\n{{}}", "x".repeat(16384));
        assert!(api_response(oversized.as_bytes()).is_err());
        assert_eq!(retry_delay("1", false).unwrap(), Duration::from_secs(1));
        assert!(retry_delay("61", false).is_err());
        assert_eq!(
            retry_delay("Thu, 01 Jan 1970 00:00:00 GMT", false).unwrap(),
            Duration::ZERO
        );
        assert!(retry_delay("Mon, 31 Apr 2023 00:00:00 GMT", false).is_err());
    }

    /// Fixture proves run/attempt/SHA/workflow/repo and terminal outcome boundaries.
    #[test]
    fn run_identity_rejects_moved_attempt_and_failed_ci() {
        let w = Workflow {
            id: WORKFLOW_ID,
            path: WORKFLOW_PATH.into(),
            run_id: 17,
            run_attempt: 2,
        };
        let mut run = json!({"id":17,"run_attempt":2,"workflow_id":WORKFLOW_ID,"path":WORKFLOW_PATH,"head_sha":"a".repeat(40),"head_branch":"v0.19.4","event":"push","repository":{"id":REPOSITORY_ID,"full_name":REPOSITORY},"status":"completed","conclusion":"success"});
        assert!(validate_run(&run, &w, "v0.19.4", &"a".repeat(40)).unwrap());
        run["run_attempt"] = json!(3);
        assert_eq!(
            validate_run(&run, &w, "v0.19.4", &"a".repeat(40))
                .unwrap_err()
                .code,
            5
        );
        run["run_attempt"] = json!(2);
        run["conclusion"] = json!("failure");
        assert_eq!(
            validate_run(&run, &w, "v0.19.4", &"a".repeat(40))
                .unwrap_err()
                .code,
            1
        );
    }
    /// A stable published marker cannot be replaced with a draft or prerelease.
    #[test]
    fn publication_and_event_do_not_claim_install_or_wake() {
        let mut release = json!({"id":17,"tag_name":"v0.19.4","draft":false,"prerelease":false,"published_at":"fixture"});
        release_identity(&release, "v0.19.4", true).unwrap();
        release["prerelease"] = json!(true);
        assert!(release_identity(&release, "v0.19.4", true).is_err());
        let temp = tempfile::tempdir().unwrap();
        let args = vec![
            "--repo".into(),
            REPOSITORY.into(),
            "--tag".into(),
            "v0.19.4".into(),
            "--commit".into(),
            "a".repeat(40),
            "--result-file".into(),
            temp.path().join("result").to_string_lossy().into(),
        ];
        let o = Options::parse(&args).unwrap();
        let one = event(&o, None, "timeout", "deadline", None);
        assert_eq!(one, event(&o, None, "timeout", "deadline", None));
        assert_eq!(one["installed"], false);
        assert_eq!(one["agent_awakened"], false);
        delivery::create_json(&o.result, &one).unwrap();
        assert!(delivery::create_json(&o.result, &one).is_err());
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            fs::metadata(&o.result).unwrap().permissions().mode() & 0o777,
            0o600
        );
    }
}
