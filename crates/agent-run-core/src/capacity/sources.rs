//! Bounded native probes. Provider response bodies and backend account IDs are never logged.
use super::{account_token, Key, Pool, Route, Sample, Slice, Topology};
use crate::{
    adapters::{
        self,
        io::Process,
        materialize::{self, Publisher},
        LaunchPlan,
    },
    config::{Adapter, Auth, Config, Runtime},
    domain::now,
    error::invalid,
    fs,
    process::OwnedProcess,
    profiles::Profile,
    Error, Result,
};
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
    process::Stdio,
    time::Duration,
};
use tokio::{io::AsyncReadExt, process::Command};
const TTL: f64 = 900.0;
/// Maximum seconds allowed for one Codexbar provider invocation.
pub const CODEXBAR_TIMEOUT_SECONDS: u64 = 120;

/// Extracts a nonblank email claim from a Codex auth document.
///
/// Malformed files, missing tokens, invalid base64url payloads, and absent or
/// empty claims all return `None`; no parse detail or token content is exposed.
pub fn account_email(path: &Path) -> Option<String> {
    let payload: Value = serde_json::from_slice(&std::fs::read(path).ok()?).ok()?;
    let token = payload.pointer("/tokens/id_token")?.as_str()?;
    let encoded = token.split('.').nth(1)?;
    let mut bytes = Vec::new();
    let mut value = 0_u32;
    let mut bits = 0_u8;
    for byte in encoded.bytes() {
        let digit = match byte {
            b'A'..=b'Z' => byte - b'A',
            b'a'..=b'z' => byte - b'a' + 26,
            b'0'..=b'9' => byte - b'0' + 52,
            b'-' => 62,
            b'_' => 63,
            _ => return None,
        } as u32;
        value = (value << 6) | digit;
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            bytes.push((value >> bits) as u8);
            value &= (1 << bits) - 1;
        }
    }
    serde_json::from_slice::<Value>(&bytes)
        .ok()?
        .get("email")?
        .as_str()
        .filter(|email| !email.is_empty())
        .map(str::to_owned)
}
const BODY_MAX: usize = 2 * 1024 * 1024;
fn number(v: Option<&Value>) -> Option<f64> {
    v.and_then(Value::as_f64).filter(|v| v.is_finite())
}
fn timestamp(v: Option<&Value>) -> Option<f64> {
    let s = v.and_then(Value::as_str)?;
    if s.len() > 64 {
        return None;
    }
    chrono::DateTime::parse_from_rfc3339(s)
        .ok()
        .map(|d| d.timestamp_millis() as f64 / 1000.0)
        .filter(|v| *v >= 0.0)
}
fn window_name(minutes: f64) -> String {
    if minutes == 300.0 {
        "five_hour".into()
    } else if minutes == 10080.0 {
        "seven_day".into()
    } else {
        format!("min{minutes}")
    }
}
// The fields intentionally mirror one provider window; grouping them would
// obscure the direct mapping and churn every source normalizer call site.
#[allow(clippy::too_many_arguments)]
fn sample(
    runtime: &str,
    lane: &str,
    window: String,
    target: Option<String>,
    source: &str,
    remaining: f64,
    reset: Option<f64>,
    observed: f64,
) -> Sample {
    Sample {
        key: Key {
            runtime: runtime.into(),
            lane: lane.into(),
            window,
            target,
            source: source.into(),
        },
        remaining_percent: Some(remaining),
        reset_at: reset,
        observed_at: Some(observed),
        valid_until: Some(observed + TTL),
    }
}
pub fn normalize_codex(
    runtime: &str,
    account: Option<&str>,
    response: &Value,
    observed: f64,
) -> Result<(Slice, Option<String>)> {
    if !observed.is_finite() || observed < 0.0 {
        return Err(invalid("invalid quota observation"));
    }
    let raw = response
        .get("result")
        .filter(|v| v.is_object())
        .unwrap_or(response);
    let buckets = if let Some(map) = raw.get("rateLimitsByLimitId").and_then(Value::as_object) {
        map.clone()
    } else if let Some(v) = raw.get("rateLimits").filter(|v| v.is_object()) {
        let name = v
            .get("limitId")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .unwrap_or("codex");
        let mut map = serde_json::Map::new();
        map.insert(name.into(), v.clone());
        map
    } else {
        return Err(invalid("codex quota response has no bucket map"));
    };
    let credits = raw
        .pointer("/rateLimitResetCredits/availableCount")
        .and_then(Value::as_u64);
    let backend = raw
        .get("accountId")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .map(str::to_owned);
    let mut samples = Vec::new();
    let mut topology = Topology::default();
    for (limit_id, bucket) in buckets {
        if limit_id.is_empty() || !bucket.is_object() {
            continue;
        }
        let mut complete = true;
        let mut keys = BTreeSet::new();
        for field in ["primary", "secondary"] {
            let Some(value) = bucket.get(field).filter(|v| !v.is_null()) else {
                continue;
            };
            let used = number(value.get("usedPercent"));
            let minutes = number(value.get("windowDurationMins"));
            let reset = number(value.get("resetsAt"));
            if used.is_none_or(|v| !(0.0..=100.0).contains(&v))
                || minutes.is_none_or(|v| v <= 0.0)
                || value
                    .get("resetsAt")
                    .is_some_and(|v| !v.is_null() && reset.is_none_or(|n| n < 0.0))
            {
                complete = false;
                continue;
            }
            let s = sample(
                runtime,
                &limit_id,
                window_name(minutes.unwrap_or(0.0)),
                account.map(str::to_owned),
                "codex_appserver",
                100.0 - used.unwrap_or(0.0),
                reset,
                observed,
            );
            if !keys.insert(s.key.clone()) {
                complete = false;
                continue;
            }
            samples.push(s);
        }
        // A malformed present window disables the WHOLE bucket, including its good sibling.
        if complete && !keys.is_empty() {
            let id = format!("{runtime}:{}:{limit_id}", account_token(account));
            topology.pools.push(Pool {
                pool_id: id.clone(),
                keys,
            });
            topology.routes.push(Route {
                route_id: id.clone(),
                runtime: runtime.into(),
                account: account.map(str::to_owned),
                quota_lane: bucket
                    .get("limitName")
                    .and_then(Value::as_str)
                    .filter(|s| !s.is_empty())
                    .unwrap_or(&limit_id)
                    .into(),
                pool_ids: vec![id],
                reset_credits: if limit_id == "codex" { credits } else { None },
            });
        }
    }
    topology.validate(runtime)?;
    Ok((
        Slice {
            runtime: runtime.into(),
            scope_id: format!("codex:{}", account_token(account)),
            samples,
            topology,
            observed_at: observed,
            valid_until: observed + TTL,
        },
        backend,
    ))
}
fn probe_profile() -> Profile {
    Profile {
        name: "probe".into(),
        body: "Read provider metadata only.".into(),
        write: false,
        network: false,
        revision: "probe-v1".into(),
        canonical: true,
        allow_external_read_roots: false,
        read_roots: vec![],
        skills: vec![],
        mcp: vec![],
        required_constraints: BTreeSet::new(),
    }
}
/// Query one bounded Codex metadata endpoint in an isolated, verified process group.
///
/// Only the two supported metadata methods are accepted. The temporary probe home is
/// removed after cleanup only when native group/descendant evidence confirms it; an
/// unavailable cleanup observation returns its typed runtime error after reaping.
pub async fn codex_probe(
    app_home: &Path,
    cfg: &Config,
    rt: &Runtime,
    account: Option<&str>,
    method: &str,
) -> Result<Value> {
    if rt.kind()? != Adapter::Codex || !["model/list", "account/rateLimits/read"].contains(&method)
    {
        return Err(invalid("invalid metadata probe"));
    }
    let probes = app_home.join("probes");
    fs::private_dir(&probes)?;
    let home = probes.join(uuid::Uuid::new_v4().to_string());
    let prepared = (|| -> Result<LaunchPlan> {
        let mut p = Publisher::new(&home)?;
        p.file(
            "config.toml",
            b"cli_auth_credentials_store = \"file\"\n",
            0o600,
        )?;
        let source = if let Some(label) = account {
            materialize::account_home(app_home, Adapter::Codex, label).join("auth.json")
        } else if let Some(Auth::FileLink { source, .. }) = &rt.auth {
            source.clone()
        } else {
            let root = std::env::var_os("CODEX_HOME")
                .map(PathBuf::from)
                .unwrap_or(fs::expand(Path::new("~/.codex"))?);
            root.join("auth.json")
        };
        p.link("auth.json", &source)?;
        p.finish()?;
        let mut clean = rt.clone();
        clean.environment = None;
        clean.rust = None;
        clean.mcp.clear();
        clean.skills.clear();
        Ok(LaunchPlan {
            binary: rt.binary.clone(),
            args: vec!["app-server".into()],
            cwd: home.clone(),
            environment: adapters::environment(
                cfg,
                &clean,
                &probe_profile(),
                &home,
                account,
                app_home,
            )?,
            initial_input: None,
        })
    })();
    let plan = match prepared {
        Ok(v) => v,
        Err(e) => {
            let _ = std::fs::remove_dir_all(&home);
            return Err(e);
        }
    };
    let mut process = match Process::spawn(&plan) {
        Ok(p) => p,
        Err(e) => {
            let _ = std::fs::remove_dir_all(&home);
            return Err(e);
        }
    };
    let result = tokio::time::timeout(
        Duration::from_secs(35),
        crate::codex::query(&mut process, method),
    )
    .await
    .map_err(|_| Error::Runtime("metadata probe timed out".into()))
    .and_then(|r| r);
    let cleanup = process.owner.cleanup(Duration::from_secs(1)).await;
    process.reap().await;
    let cleanup = cleanup?;
    if cleanup.confirmed {
        let _ = std::fs::remove_dir_all(&home);
    } else {
        return Err(Error::Runtime(
            "metadata probe cleanup was not confirmed".into(),
        ));
    }
    result
}
fn host_environment() -> BTreeMap<String, String> {
    [
        "HOME", "PATH", "USER", "LOGNAME", "LANG", "LC_ALL", "TMPDIR",
    ]
    .iter()
    .filter_map(|k| std::env::var(k).ok().map(|v| (k.to_string(), v)))
    .collect()
}
/// A bounded subprocess reader shared by metadata probes and diagnostics.
///
/// The child runs in its own process group and is reaped before this returns;
/// incomplete cleanup or an unavailable native observation is returned as an error.
pub async fn capture(
    binary: &Path,
    args: &[String],
    seconds: u64,
    environment: &BTreeMap<String, String>,
) -> Result<Vec<u8>> {
    use std::os::unix::process::CommandExt;
    let mut command = Command::new(binary);
    command
        .args(args)
        .env_clear()
        .envs(environment)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(false);
    command.as_std_mut().process_group(0);
    let mut child = command.spawn()?;
    let mut owner =
        OwnedProcess::capture(child.id().ok_or_else(|| invalid("probe PID missing"))? as i32);
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| invalid("probe output missing"))?;
    let read = async {
        let mut bytes = Vec::new();
        let mut reader = stdout.take((BODY_MAX + 1) as u64);
        reader.read_to_end(&mut bytes).await?;
        if bytes.len() > BODY_MAX {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "probe output bound",
            ));
        }
        Ok(bytes)
    };
    let result = tokio::time::timeout(Duration::from_secs(seconds), async {
        let (bytes, status) = tokio::try_join!(read, child.wait())?;
        if !status.success() {
            return Err(invalid("metadata command failed"));
        }
        Ok(bytes)
    })
    .await
    .map_err(|_| invalid("metadata command timed out"))
    .and_then(|r| r);
    let clean = owner.cleanup(Duration::from_secs(1)).await;
    let _ = tokio::time::timeout(Duration::from_secs(2), child.wait()).await;
    let clean = clean?;
    if !clean.confirmed {
        return Err(invalid("metadata command cleanup unconfirmed"));
    }
    result
}
fn conservative_topology(runtime: &str, samples: &[Sample], account: Option<&str>) -> Topology {
    // Every declared window governs the route; unknown scoped semantics cannot widen eligibility.
    let keys: BTreeSet<_> = samples.iter().map(|s| s.key.clone()).collect();
    if keys.is_empty() {
        return Topology::default();
    }
    let source = samples[0].key.source.as_str();
    let id = format!("{runtime}:{source}:{}:all", account_token(account));
    Topology {
        pools: vec![Pool {
            pool_id: id.clone(),
            keys,
        }],
        routes: vec![Route {
            route_id: id.clone(),
            runtime: runtime.into(),
            account: account.map(str::to_owned),
            quota_lane: "all".into(),
            pool_ids: vec![id],
            reset_credits: None,
        }],
    }
}
pub fn normalize_claude(runtime: &str, raw: &Value, observed: f64) -> Result<Slice> {
    let entries = raw
        .get("limits")
        .and_then(Value::as_array)
        .ok_or_else(|| invalid("claude_malformed_response"))?;
    let mut samples = Vec::new();
    for entry in entries {
        let used = number(entry.get("percent"))
            .filter(|p| (0.0..=100.0).contains(p))
            .ok_or_else(|| invalid("claude_malformed_response"))?;
        let kind = entry
            .get("kind")
            .and_then(Value::as_str)
            .ok_or_else(|| invalid("claude_malformed_response"))?;
        let (lane, window, target) = match kind {
            "session" => ("primary", "five_hour", None),
            "weekly_all" => ("secondary", "seven_day", None),
            "weekly_scoped" => (
                "secondary",
                "seven_day",
                Some(
                    entry
                        .pointer("/scope/model/display_name")
                        .and_then(Value::as_str)
                        .unwrap_or("weekly_scoped")
                        .to_lowercase(),
                ),
            ),
            other => ("secondary", "seven_day", Some(other.into())),
        };
        let reset = timestamp(entry.get("resets_at"));
        if entry.get("resets_at").is_some_and(|v| !v.is_null()) && reset.is_none() {
            return Err(invalid("claude_invalid_reset"));
        }
        samples.push(sample(
            runtime,
            lane,
            window.into(),
            target,
            "native",
            100.0 - used,
            reset,
            observed,
        ));
    }
    let topology = conservative_topology(runtime, &samples, None);
    Ok(Slice {
        runtime: runtime.into(),
        scope_id: runtime.into(),
        samples,
        topology,
        observed_at: observed,
        valid_until: observed + TTL,
    })
}
async fn claude_native(runtime: &str, rt: &Runtime) -> Result<Slice> {
    let allowed = matches!(&rt.auth,Some(Auth::Environment{names})if names.iter().any(|n|n=="CLAUDE_CODE_OAUTH_TOKEN"));
    if !allowed {
        return Err(invalid("claude_token_missing"));
    }
    let token =
        std::env::var("CLAUDE_CODE_OAUTH_TOKEN").map_err(|_| invalid("claude_token_missing"))?;
    if token.is_empty() {
        return Err(invalid("claude_token_missing"));
    }
    let observed = now();
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(20))
        .connect_timeout(Duration::from_secs(5))
        .redirect(reqwest::redirect::Policy::none())
        .no_proxy()
        .build()
        .map_err(|_| invalid("claude_usage_unreachable"))?;
    let mut response = client
        .get("https://api.anthropic.com/api/oauth/usage")
        .bearer_auth(token)
        .header("anthropic-beta", "oauth-2025-04-20")
        .send()
        .await
        .map_err(|_| invalid("claude_usage_unreachable"))?;
    if !response.status().is_success() {
        return Err(invalid("claude_usage_unreachable"));
    }
    let mut bytes = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|_| invalid("claude_usage_unreachable"))?
    {
        if bytes.len() + chunk.len() > BODY_MAX {
            return Err(invalid("claude_malformed_response"));
        }
        bytes.extend_from_slice(&chunk);
    }
    let value: Value =
        serde_json::from_slice(&bytes).map_err(|_| invalid("claude_malformed_response"))?;
    normalize_claude(runtime, &value, observed)
}
/// Reads the newest bounded Claude runtime stream as a credential-free fallback.
///
/// Only regular files below the supplied agent-run home are considered. Missing,
/// malformed, or stale stream data produces an empty slice or unknown evidence;
/// it never manufactures a percentage and never triggers a provider request.
pub fn read_claude_stream(home: &Path, runtime: &str) -> Result<Slice> {
    const MAX_FILES: usize = 24;
    const MAX_BYTES: u64 = 262_144;
    const MAX_LINES: usize = 2_048;
    let root = home.join("agents");
    let mut paths = match std::fs::read_dir(&root) {
        Ok(entries) => entries
            .filter_map(std::result::Result::ok)
            .filter(|entry| {
                entry
                    .file_type()
                    .is_ok_and(|kind| kind.is_dir() && !kind.is_symlink())
            })
            .map(|entry| entry.path().join("runtime.jsonl"))
            .filter(|path| path.is_file() && !path.is_symlink())
            .filter_map(|path| {
                path.metadata()
                    .ok()
                    .and_then(|meta| meta.modified().ok())
                    .and_then(|modified| modified.duration_since(std::time::UNIX_EPOCH).ok())
                    .map(|modified| (modified.as_secs_f64(), path))
            })
            .collect::<Vec<_>>(),
        Err(_) => Vec::new(),
    };
    paths.sort_by(|left, right| right.0.total_cmp(&left.0));
    let observed = now();
    for (modified, path) in paths.into_iter().take(MAX_FILES) {
        let Ok(mut file) = std::fs::File::open(path) else {
            continue;
        };
        use std::io::{Read, Seek, SeekFrom};
        let end = file.seek(SeekFrom::End(0)).unwrap_or(0);
        if file
            .seek(SeekFrom::Start(end.saturating_sub(MAX_BYTES)))
            .is_err()
        {
            continue;
        }
        let mut data = Vec::new();
        if file.take(MAX_BYTES).read_to_end(&mut data).is_err() {
            continue;
        }
        let text = String::from_utf8(data).unwrap_or_default();
        for line in text.lines().rev().take(MAX_LINES) {
            if !line.contains("\"rate_limit_event\"") {
                continue;
            }
            let Ok(event) = serde_json::from_str::<Value>(line) else {
                continue;
            };
            if event.get("type").and_then(Value::as_str) != Some("rate_limit_event") {
                continue;
            }
            let Some(windows) = event
                .pointer("/rate_limit_info/unifiedWindows")
                .and_then(Value::as_object)
            else {
                continue;
            };
            let stale = observed - modified > TTL;
            let mut samples = Vec::new();
            for (window, entry) in windows {
                let Some(utilization) = number(entry.get("utilization")) else {
                    continue;
                };
                let reset = entry
                    .get("resetsAt")
                    .and_then(Value::as_f64)
                    .filter(|value| value.is_finite() && *value >= 0.0);
                samples.push(Sample {
                    key: Key {
                        runtime: runtime.into(),
                        lane: "usage".into(),
                        window: window.clone(),
                        target: None,
                        source: if stale {
                            "unknown".into()
                        } else {
                            "runtime_stream_evidence".into()
                        },
                    },
                    remaining_percent: (!stale)
                        .then_some((1.0 - utilization).mul_add(100.0, 0.0).clamp(0.0, 100.0)),
                    reset_at: reset,
                    observed_at: Some(modified),
                    valid_until: Some(modified + TTL),
                });
            }
            if !samples.is_empty() {
                let topology = conservative_topology(runtime, &samples, None);
                return Ok(Slice {
                    runtime: runtime.into(),
                    scope_id: runtime.into(),
                    samples,
                    topology,
                    observed_at: modified,
                    valid_until: modified + TTL,
                });
            }
        }
    }
    Ok(Slice {
        runtime: runtime.into(),
        scope_id: runtime.into(),
        samples: Vec::new(),
        topology: Topology::default(),
        observed_at: observed,
        valid_until: observed + TTL,
    })
}
pub fn normalize_codexbar(runtime: &str, raw: &Value) -> Result<Slice> {
    normalize_codexbar_accounts(runtime, raw, &BTreeMap::new(), None)
}
/// Normalizes one Codexbar payload and maps each discovered email to its route account.
///
/// `accounts` maps configured labels to auth-file email claims; `default_email`
/// remains the unlabelled target. Unknown provider accounts retain evidence but
/// receive no route, so they cannot become launchable identities.
pub fn normalize_codexbar_accounts(
    runtime: &str,
    raw: &Value,
    accounts: &BTreeMap<String, String>,
    default_email: Option<&str>,
) -> Result<Slice> {
    let value = if let Some(list) = raw.as_array() {
        if accounts.is_empty() {
            list.first().map(std::slice::from_ref).unwrap_or(&[])
        } else {
            list
        }
    } else {
        std::slice::from_ref(raw)
    };
    let mut samples = Vec::new();
    for entry in value {
        let usage = entry
            .get("usage")
            .filter(|value| value.is_object())
            .ok_or_else(|| invalid("codexbar_malformed_response"))?;
        let email = usage
            .pointer("/identity/accountEmail")
            .or_else(|| usage.get("accountEmail"))
            .and_then(Value::as_str)
            .filter(|email| !email.is_empty());
        let target = if accounts.is_empty() {
            None
        } else if email.is_none() {
            Some("unknown".to_owned())
        } else if email == default_email {
            None
        } else {
            Some(
                accounts
                    .iter()
                    .find_map(|(label, known)| {
                        (Some(known.as_str()) == email).then(|| label.clone())
                    })
                    .unwrap_or_else(|| email.unwrap_or_default().to_owned()),
            )
        };
        let observed = timestamp(usage.get("updatedAt"))
            .ok_or_else(|| invalid("codexbar_invalid_observed_at"))?;
        for lane in ["primary", "secondary", "tertiary"] {
            let Some(value) = usage.get(lane).filter(|value| !value.is_null()) else {
                continue;
            };
            let used = number(value.get("usedPercent"))
                .filter(|percent| (0.0..=100.0).contains(percent))
                .ok_or_else(|| invalid("codexbar_invalid_window"))?;
            let minutes = number(value.get("windowMinutes"))
                .filter(|minutes| *minutes > 0.0)
                .ok_or_else(|| invalid("codexbar_invalid_window"))?;
            let reset = timestamp(value.get("resetsAt"));
            if value.get("resetsAt").is_some_and(|value| !value.is_null()) && reset.is_none() {
                return Err(invalid("codexbar_invalid_reset"));
            }
            samples.push(sample(
                runtime,
                lane,
                window_name(minutes),
                target.clone(),
                "codexbar",
                100.0 - used,
                reset,
                observed,
            ));
        }
    }
    if samples.is_empty() {
        return Err(invalid("codexbar_missing_data"));
    }
    let mut topology = Topology::default();
    for (target, samples) in samples.iter().fold(
        BTreeMap::<Option<String>, Vec<&Sample>>::new(),
        |mut groups, sample| {
            groups
                .entry(sample.key.target.clone())
                .or_default()
                .push(sample);
            groups
        },
    ) {
        let keys = samples
            .into_iter()
            .map(|sample| sample.key.clone())
            .collect();
        let id = format!(
            "{runtime}:codexbar:{}:all",
            account_token(target.as_deref())
        );
        topology.pools.push(Pool {
            pool_id: id.clone(),
            keys,
        });
        if target
            .as_ref()
            .is_none_or(|target| accounts.contains_key(target))
        {
            topology.routes.push(Route {
                route_id: id.clone(),
                runtime: runtime.into(),
                account: target,
                quota_lane: "default".into(),
                pool_ids: vec![id],
                reset_credits: None,
            });
        }
    }
    topology.validate(runtime)?;
    let observed_at = samples
        .iter()
        .map(|sample| sample.observed_at.unwrap_or(0.0))
        .fold(0.0, f64::max);
    Ok(Slice {
        runtime: runtime.into(),
        scope_id: runtime.into(),
        samples,
        topology,
        observed_at,
        valid_until: observed_at + TTL,
    })
}
async fn codexbar(home: &Path, cfg: &Config, name: &str, rt: &Runtime) -> Result<Slice> {
    let provider = match rt.kind()? {
        Adapter::Codex => "codex",
        Adapter::Claude => "claude",
        Adapter::Glm => "zai",
        Adapter::Qwen => return Err(Error::Unsupported("codexbar has no Qwen provider".into())),
    };
    let mut args = vec![
        "usage".into(),
        "--provider".into(),
        provider.into(),
        "--json".into(),
    ];
    if !rt.accounts.is_empty() {
        args.push("--all-accounts".into());
    }
    if rt.kind()? == Adapter::Claude {
        args.extend(["--source".into(), "cli".into()]);
    }
    let bytes = capture(
        &cfg.capacity.codexbar_binary,
        &args,
        CODEXBAR_TIMEOUT_SECONDS,
        &host_environment(),
    )
    .await?;
    let value: Value =
        serde_json::from_slice(&bytes).map_err(|_| invalid("codexbar_malformed_response"))?;
    let accounts = rt
        .accounts
        .iter()
        .filter_map(|label| {
            account_email(
                &home
                    .join("accounts")
                    .join(name)
                    .join(label)
                    .join("auth.json"),
            )
            .map(|email| (label.clone(), email))
        })
        .collect();
    let default_email = match &rt.auth {
        Some(Auth::FileLink { source, .. }) => account_email(source),
        _ => None,
    };
    normalize_codexbar_accounts(name, &value, &accounts, default_email.as_deref())
}
/// Reads the local OmniRoute current-cache into one capacity slice.
///
/// The route has one physical pool for the shared `opencode-go` capacity.
/// Docker failures and malformed cache values propagate fixed unavailable
/// reasons, while a healthy empty cache remains a no-data outcome.
async fn omniroute(name: &str) -> Result<Slice> {
    let args = vec![
        "exec".into(),
        "omniroute".into(),
        "node".into(),
        "-e".into(),
        super::omniroute::script(),
    ];
    let environment = host_environment();
    let attempt = || capture(super::omniroute::docker(), &args, 10, &environment);
    let values = match super::omniroute::read(attempt()).await {
        Ok(values) => values,
        Err(Error::Runtime(reason)) if reason == "omniroute_unavailable" => {
            tokio::time::sleep(Duration::from_secs(2)).await;
            super::omniroute::read(attempt()).await?
        }
        Err(error) => return Err(error),
    };
    let samples = values
        .into_iter()
        .map(|mut sample| {
            sample.key.runtime = name.into();
            sample
        })
        .collect::<Vec<_>>();
    let observed_at = samples
        .iter()
        .filter_map(|sample| sample.observed_at)
        .fold(0.0, f64::max);
    let topology = conservative_topology(name, &samples, None);
    Ok(Slice {
        runtime: name.into(),
        scope_id: name.into(),
        samples,
        topology,
        observed_at,
        valid_until: observed_at + super::omniroute::STALE_SECONDS,
    })
}
pub async fn collect(home: &Path) -> Result<Value> {
    let config = Config::load(home)?;
    let started = now();
    let mut results = Vec::new();
    let mut all_ok = true;
    for (name, rt) in config.runtimes.iter().filter(|(_, r)| r.enabled) {
        let source = rt.limits_source.as_deref().unwrap_or("native");
        let mut count = 0;
        let mut issues: Vec<String> = Vec::new();
        let mut no_data = false;
        if source == "none" {
            results.push(json!({"runtime":name,"status":"unsupported","sample_count":0,"error":null,"issues":[]}));
            continue;
        }
        if (source == "codex_appserver" || source == "native") && rt.kind()? == Adapter::Codex {
            let mut scopes = vec![None];
            scopes.extend(rt.accounts.iter().map(|s| Some(s.as_str())));
            let mut seen = BTreeSet::new();
            for account in scopes {
                let observed = now();
                let response =
                    codex_probe(home, &config, rt, account, "account/rateLimits/read").await;
                match response.and_then(|v| normalize_codex(name, account, &v, observed)) {
                    Ok((slice, backend)) => {
                        if let Some(id) = backend {
                            if !seen.insert(id) {
                                continue;
                            }
                        }
                        if slice.samples.is_empty() {
                            issues.push("scope_empty".into());
                            continue;
                        }
                        if slice.topology.routes.is_empty() {
                            issues.push("incomplete_bucket".into());
                        }
                        match super::persist(home, &slice, config.capacity.sample_retention) {
                            Ok(n) => count += n,
                            Err(_) => issues.push("persist_failed".into()),
                        }
                    }
                    Err(_) => issues.push("probe_failed".into()),
                }
            }
        } else {
            let result = match (source, rt.kind()?) {
                ("codexbar", _) => codexbar(home, &config, name, rt).await,
                ("omniroute", _) => omniroute(name).await,
                ("native", Adapter::Qwen) => omniroute(name).await,
                ("native", Adapter::Claude) => match claude_native(name, rt).await {
                    Ok(slice) => Ok(slice),
                    Err(Error::Validation(reason)) if reason == "claude_token_missing" => {
                        read_claude_stream(home, name)
                    }
                    Err(error) => Err(error),
                },
                _ => Err(Error::Unsupported(
                    "selected quota source has not been ported".into(),
                )),
            };
            match result {
                Ok(slice) if slice.samples.is_empty() => no_data = true,
                Ok(slice) => match super::persist(home, &slice, config.capacity.sample_retention) {
                    Ok(n) => count = n,
                    Err(_) => issues.push("persist_failed".into()),
                },
                Err(Error::Unsupported(_)) => issues.push("source_not_ported".into()),
                Err(Error::Validation(reason)) => issues.push(reason),
                Err(Error::Runtime(reason)) => issues.push(reason),
                Err(_) => issues.push("source_failed".into()),
            }
        }
        let status = if issues.is_empty() && count > 0 {
            "collected"
        } else if count > 0 {
            "partial"
        } else if no_data && issues.is_empty() {
            "no_data"
        } else {
            "failed"
        };
        if status != "collected" {
            all_ok = false;
        }
        results.push(json!({"runtime":name,"status":status,"sample_count":count,"error":issues.first(),"issues":issues}));
    }
    Ok(
        json!({"started_at":started,"finished_at":now(),"ok":all_ok,"results":results,"over_interval":now()-started>config.capacity.collect_interval_seconds as f64}),
    )
}
pub async fn models(home: &Path) -> Result<Value> {
    let cfg = Config::load(home)?;
    let mut result = serde_json::Map::new();
    for (name, rt) in cfg.runtimes.iter().filter(|(_, r)| r.enabled) {
        let kind = rt.kind()?;
        let mut roster: BTreeMap<String, Value> = BTreeMap::new();
        let mut accounts = Vec::new();
        let mut reason = None;
        if kind == Adapter::Codex {
            let mut scopes = vec![None];
            scopes.extend(rt.accounts.iter().map(|s| Some(s.as_str())));
            for account in scopes {
                match codex_probe(home, &cfg, rt, account, "model/list").await {
                    Ok(value) => {
                        let mut ids = Vec::new();
                        if let Some(data) = value.get("data").and_then(Value::as_array) {
                            for model in data {
                                if let Some(id) = model
                                    .get("model")
                                    .or_else(|| model.get("id"))
                                    .and_then(Value::as_str)
                                    .filter(|s| rt.models.iter().any(|m| m == s))
                                {
                                    let efforts: Vec<_> = model
                                        .get("supportedReasoningEfforts")
                                        .and_then(Value::as_array)
                                        .into_iter()
                                        .flatten()
                                        .filter_map(|v| {
                                            v.get("reasoningEffort").and_then(Value::as_str)
                                        })
                                        .collect();
                                    roster.insert(id.into(),json!({"id":id,"description":"configured and observed in native account roster","efforts":efforts}));
                                    ids.push(id.to_owned());
                                }
                            }
                        }
                        accounts.push(
                            json!({"account":account,"available":!ids.is_empty(),"models":ids}),
                        );
                    }
                    Err(_) => {
                        reason = Some("one or more account probes failed");
                        accounts.push(json!({"account":account,"available":false,"models":[],"reason":"probe_failed"}));
                    }
                }
            }
        } else {
            if capture(&rt.binary, &["--version".into()], 5, &host_environment())
                .await
                .is_ok()
            {
                for id in &rt.models {
                    roster.insert(id.clone(),json!({"id":id,"description":"configured native CLI model; authentication checked on launch","efforts":if kind==Adapter::Qwen{vec![]}else{vec!["low","medium","high","xhigh","max"]}}));
                }
            } else {
                reason = Some("configured binary probe failed");
            }
        }
        if roster.is_empty() && reason.is_none() {
            reason = Some("roster empty");
        }
        let available = !roster.is_empty();
        result.insert(name.clone(),json!({"models":roster.into_values().collect::<Vec<_>>(),"capabilities":adapters::capabilities(kind),"available":available,"reason":reason,"accounts":accounts}));
    }
    Ok(Value::Object(result))
}
