//! Read-only Agent Quota transport v1 and snapshot schema v2 client.
//! Each call owns one authenticated socket.
//!
//! Facts are returned exactly as reported, including nulls and per-window times.
//! The service UID and its socket directory are administrative trust boundaries.

use serde::{Deserialize, Deserializer, Serialize};
use std::{
    collections::BTreeSet,
    fmt,
    os::unix::fs::{FileTypeExt, MetadataExt},
    path::{Path, PathBuf},
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::UnixStream,
    time::{Instant, timeout_at},
};

const MAX_REQUEST: usize = 64 * 1024;
const MAX_RESPONSE: usize = 4 * 1024 * 1024;
const EXCHANGE_DEADLINE: Duration = Duration::from_secs(5);

/// A safe failure category: no input, peer payload, paths, or OS errors are exposed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Error {
    InvalidRequest,
    UnsafeSocket,
    Unavailable,
    Timeout,
    InvalidResponse,
    Service(ServiceError),
}

/// Service-reported failures relevant to a read; unknown codes stay failures.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ServiceError {
    ResponseTooLarge,
    HistoryTooLarge,
    UnknownAccount,
    UnknownModel,
    Other,
}

impl fmt::Display for Error {
    /// Describes only the failure category, never values from the request or response.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "quota client: {self:?}")
    }
}
impl std::error::Error for Error {}

/// Schema-v2 snapshot; a null revision means no accounts were returned.
#[derive(Deserialize)]
pub struct Snapshot {
    pub schema_version: u8,
    pub source_epoch: String,
    #[serde(deserialize_with = "required_option")]
    pub revision: Option<i64>,
    pub config_revision: String,
    pub evaluated_at: f64,
    pub coverage: Coverage,
    pub accounts: Vec<Account>,
}

/// Service-reported completeness, independent of the freshness of individual windows.
#[derive(Deserialize)]
pub struct Coverage {
    pub state: CoverageState,
    pub unobserved: Vec<Unobserved>,
    #[serde(deserialize_with = "required_option")]
    pub last_collection: Option<Collection>,
}

/// Coverage state understood by protocol v1.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum CoverageState {
    Unknown,
    Partial,
    Complete,
}

/// A configured source that has not yet provided an observation.
#[derive(Deserialize)]
pub struct Unobserved {
    pub account: String,
    pub provider: String,
    #[serde(default)]
    pub source: Option<String>,
    pub reason: UnobservedReason,
}

/// Reasons specified by protocol v1 for an unobserved source.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum UnobservedReason {
    LimitsSourceNone,
    ExternalNotReceived,
}

/// The last collection round, which may describe accounts outside this request.
#[derive(Deserialize)]
pub struct Collection {
    pub observed_at: f64,
    pub ok: bool,
    #[serde(deserialize_with = "required_option")]
    pub error: Option<String>,
    #[serde(deserialize_with = "required_option")]
    pub config_revision: Option<String>,
    pub sources: Vec<CollectionSource>,
}

/// One source's report in the last collection round.
#[derive(Deserialize)]
pub struct CollectionSource {
    pub account: String,
    pub provider: String,
    pub source: String,
    pub status: CollectionStatus,
    pub windows: usize,
    pub issues: Vec<String>,
}

/// Source statuses specified by protocol v1.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum CollectionStatus {
    External,
    NoData,
    Collected,
    Failed,
}

/// One account's model views and physical window facts.
#[derive(Deserialize)]
pub struct Account {
    pub account: String,
    pub models: Vec<Model>,
    pub windows: Vec<Window>,
    pub latches: Vec<Latch>,
    pub advice: Vec<Advice>,
    pub advice_key: String,
}

/// One configured model alias; null fields remain unknown.
#[derive(Deserialize)]
pub struct Model {
    pub provider: String,
    pub model: String,
    pub native_model: String,
    pub state: ModelState,
    pub state_validity: StateValidity,
    #[serde(deserialize_with = "required_option")]
    pub quota_score: Option<f64>,
    #[serde(deserialize_with = "required_option")]
    pub limiting_window: Option<LimitingWindow>,
    #[serde(deserialize_with = "required_option")]
    pub remaining_percent: Option<f64>,
    #[serde(deserialize_with = "required_option")]
    pub burn_percent_per_hour: Option<f64>,
    #[serde(deserialize_with = "required_option")]
    pub reset_at: Option<f64>,
    pub risk: Risk,
    pub evidence: Evidence,
    #[serde(deserialize_with = "required_option")]
    pub newest_observed_at: Option<f64>,
    #[serde(deserialize_with = "required_option")]
    pub exhausted_until: Option<f64>,
    pub pools: Vec<String>,
}

/// Capacity state returned for a model.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum ModelState {
    Known,
    Unknown,
    Exhausted,
}

/// Time validity of the reported state class, not of the numerical score or risk.
#[derive(Clone, Copy, Debug, Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum StateValidity {
    /// The exclusive Unix-seconds boundary after which the class is unknown.
    Until { at: f64 },
    /// An active exhaustion latch has no clock reset in this snapshot.
    LatchedNoReset,
    /// The state is already unknown and gives no decision evidence.
    Unknown,
}

/// Qualitative risk supplied by the quota service.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum Risk {
    Unknown,
    Low,
    Medium,
    High,
}

/// Evidence classification supplied by the quota service.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum Evidence {
    Fresh,
    Stale,
    Missing,
}

/// Identity of the window or latch limiting a model.
#[derive(Deserialize)]
pub struct LimitingWindow {
    pub pool: String,
    pub source: String,
    pub window: String,
}

/// One physical window with its own exact membership and validity times.
#[derive(Deserialize)]
pub struct Window {
    pub runtime: String,
    pub lane: String,
    pub pool: String,
    pub source: String,
    pub window: String,
    #[serde(deserialize_with = "required_option")]
    pub target: Option<String>,
    #[serde(deserialize_with = "required_option")]
    pub models: Option<Vec<String>>,
    #[serde(deserialize_with = "required_option")]
    pub observed_at: Option<f64>,
    #[serde(deserialize_with = "required_option")]
    pub valid_until: Option<f64>,
    #[serde(deserialize_with = "required_option")]
    pub last_remaining_percent: Option<f64>,
    /// Latest sample's reset time in Unix seconds, including when stale.
    #[serde(deserialize_with = "required_option")]
    pub last_reset_at: Option<f64>,
    pub forecast: Forecast,
    #[serde(deserialize_with = "required_option")]
    pub evaluation: Option<Evaluation>,
}

/// Persistent exhaustion evidence; absent membership is distinct from an empty list.
#[derive(Deserialize)]
pub struct Latch {
    pub lane: String,
    pub pool: String,
    pub source: String,
    pub window: String,
    pub observed_at: f64,
    #[serde(deserialize_with = "required_option")]
    pub reset_at: Option<f64>,
    #[serde(deserialize_with = "required_option")]
    pub models: Option<Vec<String>>,
    pub active: bool,
}

/// Quota service's window identity; timestamps elsewhere use Unix seconds.
#[derive(Deserialize)]
pub struct WindowKey {
    pub runtime: String,
    pub lane: String,
    pub window: String,
    #[serde(deserialize_with = "required_option")]
    pub target: Option<String>,
    pub source: String,
}

/// Raw forecast fields; rates use percentage points per hour.
#[derive(Deserialize)]
pub struct Forecast {
    pub key: WindowKey,
    pub known: bool,
    #[serde(deserialize_with = "required_option")]
    pub remaining_percent: Option<f64>,
    #[serde(deserialize_with = "required_option")]
    pub reset_at: Option<f64>,
    #[serde(deserialize_with = "required_option")]
    pub observed_at: Option<f64>,
    pub warmup: bool,
    #[serde(deserialize_with = "required_option")]
    pub burn_percent_per_hour: Option<f64>,
    #[serde(deserialize_with = "required_option")]
    pub sustainable_percent_per_hour: Option<f64>,
    pub risk: Risk,
    #[serde(deserialize_with = "required_option")]
    pub burn_span_seconds: Option<f64>,
}

/// Service-produced explanation; this client does not recalculate its forecast.
#[derive(Deserialize)]
pub struct Evaluation {
    pub key: WindowKey,
    pub remaining_percent: f64,
    #[serde(deserialize_with = "required_option")]
    pub burn_percent_per_hour: Option<f64>,
    #[serde(deserialize_with = "required_option")]
    pub burn_span_seconds: Option<f64>,
    #[serde(deserialize_with = "required_option")]
    pub reset_at: Option<f64>,
    #[serde(deserialize_with = "required_option")]
    pub projected_percent: Option<f64>,
    pub slack: f64,
    pub marker: Marker,
    pub risk: Risk,
}

/// Window explanation markers specified by protocol v1.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum Marker {
    Projected,
    Warmup,
    ThinEvidence,
    NoReset,
}

/// Service-provided advisory information for one window.
#[derive(Deserialize)]
pub struct Advice {
    pub key: WindowKey,
    pub known: bool,
    #[serde(deserialize_with = "required_option")]
    pub remaining_percent: Option<f64>,
    #[serde(deserialize_with = "required_option")]
    pub reset_at: Option<f64>,
    pub warmup: bool,
    pub risk: Risk,
    pub recommendations: Vec<String>,
}

/// Requires an explicitly present field even when its value is null.
fn required_option<'de, D, T>(d: D) -> Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::deserialize(d)
}

#[derive(Serialize)]
struct Request<'a> {
    version: u8,
    method: &'static str,
    accounts: &'a [String],
    #[serde(skip_serializing_if = "Option::is_none")]
    model: Option<&'a str>,
}

#[derive(Deserialize)]
#[serde(untagged, deny_unknown_fields)]
enum Reply {
    Result { version: u8, result: Snapshot },
    Error { version: u8, error: String },
}

/// Reads one snapshot using the caller's UID unless an explicit service UID is supplied.
/// A single five-second deadline covers authentication, request, response and validation.
/// Empty `accounts` requests all configured accounts; it does not assert a configured set.
/// Timed-out blocking filesystem inspection or parsing may finish later without side effects.
pub async fn snapshot(
    path: &Path,
    expected_uid: Option<u32>,
    accounts: &[String],
    model: Option<&str>,
) -> Result<Snapshot, Error> {
    let deadline = Instant::now() + EXCHANGE_DEADLINE;
    if accounts.len() > 256
        || accounts.iter().any(|id| !account_id(id))
        || model.is_some_and(|name| {
            name.is_empty() || name.len() > 256 || name.chars().any(char::is_control)
        })
    {
        return Err(Error::InvalidRequest);
    }
    let request = Request {
        version: 1,
        method: "snapshot",
        accounts,
        model,
    };
    let mut bytes = serde_json::to_vec(&request).map_err(|_| Error::InvalidRequest)?;
    if bytes.len() >= MAX_REQUEST {
        return Err(Error::InvalidRequest);
    }
    bytes.push(b'\n');
    // SAFETY: geteuid has no arguments and only reads the process identity.
    let uid = expected_uid.unwrap_or_else(|| unsafe { libc::geteuid() });
    let scope = accounts.to_vec();
    let model = model.map(str::to_owned);
    let path = path.to_owned();
    timeout_at(deadline, async move {
        let before = inspect_path(path.clone(), uid).await?;
        let mut stream = UnixStream::connect(&path)
            .await
            .map_err(|_| Error::Unavailable)?;
        authenticate_peer(&stream, uid)?;
        if inspect_path(path, uid).await? != before {
            return Err(Error::UnsafeSocket);
        }
        stream
            .write_all(&bytes)
            .await
            .map_err(|_| Error::Unavailable)?;
        stream.shutdown().await.map_err(|_| Error::Unavailable)?;
        let frame = read_response(&mut stream).await?;
        tokio::task::spawn_blocking(move || decode(&frame, &scope, model.as_deref()))
            .await
            .map_err(|_| Error::Unavailable)?
    })
    .await
    .map_err(|_| Error::Timeout)?
}

/// Confirms the kernel-reported server identity before sending any request bytes.
fn authenticate_peer(stream: &UnixStream, uid: u32) -> Result<(), Error> {
    if stream.peer_cred().map_err(|_| Error::Unavailable)?.uid() != uid {
        return Err(Error::UnsafeSocket);
    }
    Ok(())
}

/// Checks the final socket without following a symlink, off the async executor.
async fn inspect_path(path: PathBuf, uid: u32) -> Result<(u64, u64), Error> {
    tokio::task::spawn_blocking(move || {
        let meta = std::fs::symlink_metadata(&path).map_err(|_| Error::Unavailable)?;
        if !meta.file_type().is_socket() || meta.uid() != uid {
            return Err(Error::UnsafeSocket);
        }
        Ok((meta.dev(), meta.ino()))
    })
    .await
    .map_err(|_| Error::Unavailable)?
}

/// Reads one newline-terminated frame within the budget, rejecting buffered trailing bytes.
async fn read_response(stream: &mut UnixStream) -> Result<Vec<u8>, Error> {
    let mut frame = Vec::new();
    let mut chunk = [0_u8; 4096];
    loop {
        let remaining = MAX_RESPONSE.saturating_sub(frame.len());
        if remaining == 0 {
            return Err(Error::InvalidResponse);
        }
        let take = remaining.min(chunk.len());
        let count = stream
            .read(&mut chunk[..take])
            .await
            .map_err(|_| Error::Unavailable)?;
        if count == 0 {
            return Err(Error::InvalidResponse);
        }
        if let Some(end) = chunk[..count].iter().position(|byte| *byte == b'\n') {
            if end + 1 != count {
                return Err(Error::InvalidResponse);
            }
            frame.extend_from_slice(&chunk[..end]);
            return Ok(frame);
        } else {
            frame.extend_from_slice(&chunk[..count]);
        }
    }
}

/// Decodes and validates the full v1 envelope without retaining parser diagnostics.
fn decode(bytes: &[u8], accounts: &[String], model: Option<&str>) -> Result<Snapshot, Error> {
    let reply: Reply = serde_json::from_slice(bytes).map_err(|_| Error::InvalidResponse)?;
    match reply {
        Reply::Result { version: 1, result } if valid_snapshot(&result, accounts, model) => {
            Ok(result)
        }
        Reply::Error { version: 1, error } => Err(Error::Service(match error.as_str() {
            "response_too_large" => ServiceError::ResponseTooLarge,
            "history_too_large" => ServiceError::HistoryTooLarge,
            "unknown_account" => ServiceError::UnknownAccount,
            "unknown_model" => ServiceError::UnknownModel,
            _ => ServiceError::Other,
        })),
        _ => Err(Error::InvalidResponse),
    }
}

/// Checks the unchanged lowercase-led global account identifier.
fn account_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 96
        && value.as_bytes()[0].is_ascii_lowercase()
        && value
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b"_.-".contains(&b))
}

/// Checks a case-sensitive Run provider identifier without normalizing it.
fn provider_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value.as_bytes()[0].is_ascii_alphanumeric()
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"_.-".contains(&b))
}

/// Validates model names without treating their contents as diagnostics.
fn model_name(value: &str) -> bool {
    !value.trim().is_empty() && value.len() <= 256 && !value.chars().any(char::is_control)
}

/// Validates stored identity text without inventing additional format rules.
fn label(value: &str, max: usize) -> bool {
    !value.trim().is_empty() && value.len() <= max && !value.contains('\0')
}

/// Accepts a reported percentage or an explicit unknown, never an invented value.
fn percent(value: Option<f64>) -> bool {
    value.is_none_or(|n| n.is_finite() && (0.0..=100.0).contains(&n))
}

/// Checks a lowercase hexadecimal service identity of the expected width.
fn hex(value: &str, width: usize) -> bool {
    value.len() == width
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

/// Preserves the null-versus-empty distinction while checking explicit membership identities.
fn membership(values: &Option<Vec<String>>) -> bool {
    values.as_ref().is_none_or(|values| {
        let mut unique = BTreeSet::new();
        values
            .iter()
            .all(|value| model_name(value) && unique.insert(value))
    })
}

/// Validates the service's window key against its account and enclosing window.
fn key_matches(key: &WindowKey, account: &str, lane: &str, source: &str, window: &str) -> bool {
    key.runtime == account
        && key.lane == lane
        && key.source == source
        && key.window == window
        && key.target.is_none()
}

/// Verifies identity, complete scope and nullable state without computing quota policy.
fn valid_snapshot(s: &Snapshot, requested: &[String], filter: Option<&str>) -> bool {
    if s.schema_version != 2
        || !hex(&s.source_epoch, 32)
        || !hex(&s.config_revision, 64)
        || !s.evaluated_at.is_finite()
        || s.evaluated_at < 0.0
        || s.revision.is_some_and(|n| n < 0)
        || s.revision.is_some() == s.accounts.is_empty()
    {
        return false;
    }
    let mut returned = BTreeSet::new();
    for account in &s.accounts {
        if !account_id(&account.account)
            || !returned.insert(account.account.as_str())
            || !valid_account(account, filter)
        {
            return false;
        }
    }
    if !requested.is_empty() && returned != requested.iter().map(String::as_str).collect() {
        return false;
    }
    if filter.is_some() && !s.accounts.iter().any(|a| !a.models.is_empty()) {
        return false;
    }
    if !s.coverage.unobserved.iter().all(|u| {
        returned.contains(u.account.as_str())
            && provider_id(&u.provider)
            && match u.reason {
                UnobservedReason::LimitsSourceNone => u.source.is_none(),
                UnobservedReason::ExternalNotReceived => {
                    u.source.as_deref().is_some_and(|v| label(v, 128))
                }
            }
    }) {
        return false;
    }
    let round = s.coverage.last_collection.as_ref();
    if !round.is_none_or(|r| {
        r.observed_at.is_finite()
            && r.config_revision.as_deref().is_none_or(|v| hex(v, 64))
            && r.sources.iter().all(|source| {
                account_id(&source.account)
                    && provider_id(&source.provider)
                    && label(&source.source, 128)
            })
    }) {
        return false;
    }
    match s.coverage.state {
        CoverageState::Unknown => round.is_none(),
        CoverageState::Partial => round.is_some(),
        CoverageState::Complete => {
            round.is_some_and(|r| r.ok && r.config_revision.as_deref() == Some(&s.config_revision))
                && s.coverage.unobserved.is_empty()
        }
    }
}

/// Checks each account's model identities, exact window identities and nullable facts.
fn valid_account(a: &Account, filter: Option<&str>) -> bool {
    if !hex(&a.advice_key, 32) {
        return false;
    }
    let available_limits: BTreeSet<_> = a
        .windows
        .iter()
        .map(|w| (&w.pool, &w.source, &w.window))
        .chain(
            a.latches
                .iter()
                .filter(|l| l.active)
                .map(|l| (&l.pool, &l.source, &l.window)),
        )
        .collect();
    let mut models = BTreeSet::new();
    for m in &a.models {
        let mut pools = BTreeSet::new();
        if !provider_id(&m.provider)
            || !model_name(&m.model)
            || !model_name(&m.native_model)
            || !models.insert((&m.provider, &m.model, &m.native_model))
            || filter.is_some_and(|f| f != m.model && f != m.native_model)
            || !m
                .pools
                .iter()
                .all(|pool| valid_pool(pool, &a.account) && pools.insert(pool))
            || !percent(m.remaining_percent)
        {
            return false;
        }
        match m.state {
            ModelState::Known
                if m.quota_score.is_some()
                    && m.limiting_window.is_some()
                    && matches!(m.state_validity, StateValidity::Until { at } if at.is_finite() && at >= 0.0) =>
                {}
            ModelState::Exhausted
                if m.quota_score.is_none()
                    && m.limiting_window.is_some()
                    && match m.state_validity {
                        StateValidity::Until { at } => at.is_finite() && at >= 0.0,
                        StateValidity::LatchedNoReset => true,
                        StateValidity::Unknown => false,
                    } => {}
            ModelState::Unknown
                if m.quota_score.is_none()
                    && m.limiting_window.is_none()
                    && m.remaining_percent.is_none()
                    && m.burn_percent_per_hour.is_none()
                    && m.reset_at.is_none()
                    && m.state_validity == StateValidity::Unknown => {}
            _ => return false,
        }
        if let Some(limit) = &m.limiting_window
            && (!valid_pool(&limit.pool, &a.account)
                || !label(&limit.source, 128)
                || !label(&limit.window, 128)
                || !m.pools.contains(&limit.pool)
                || !available_limits.contains(&(&limit.pool, &limit.source, &limit.window)))
        {
            return false;
        }
    }
    let mut windows = BTreeSet::new();
    for w in &a.windows {
        if !provider_id(&w.runtime)
            || !label(&w.lane, 128)
            || w.pool != format!("{}::{}", a.account, w.lane)
            || !label(&w.source, 128)
            || !label(&w.window, 128)
            || !membership(&w.models)
            || !percent(w.last_remaining_percent)
            || !percent(w.forecast.remaining_percent)
            || !windows.insert((&w.lane, &w.source, &w.window))
            || !key_matches(&w.forecast.key, &a.account, &w.lane, &w.source, &w.window)
            || w.evaluation
                .as_ref()
                .is_some_and(|e| !key_matches(&e.key, &a.account, &w.lane, &w.source, &w.window))
            || (w.forecast.known
                && (w.forecast.remaining_percent.is_none() || w.forecast.risk == Risk::Unknown))
            || (!w.forecast.known
                && (w.forecast.remaining_percent.is_some()
                    || w.forecast.reset_at.is_some()
                    || w.forecast.observed_at.is_some()
                    || w.forecast.burn_percent_per_hour.is_some()
                    || w.forecast.sustainable_percent_per_hour.is_some()
                    || w.forecast.burn_span_seconds.is_some()
                    || !w.forecast.warmup
                    || w.forecast.risk != Risk::Unknown
                    || w.evaluation.is_some()))
            || w.evaluation
                .as_ref()
                .is_some_and(|e| !percent(Some(e.remaining_percent)))
        {
            return false;
        }
    }
    let mut latches = BTreeSet::new();
    for l in &a.latches {
        if !label(&l.lane, 128)
            || l.pool != format!("{}::{}", a.account, l.lane)
            || !label(&l.source, 128)
            || !label(&l.window, 128)
            || !membership(&l.models)
            || !latches.insert((&l.lane, &l.source, &l.window))
        {
            return false;
        }
    }
    a.advice.iter().all(|v| {
        percent(v.remaining_percent)
            && windows.contains(&(&v.key.lane, &v.key.source, &v.key.window))
            && key_matches(
                &v.key,
                &a.account,
                &v.key.lane,
                &v.key.source,
                &v.key.window,
            )
    })
}

/// Checks the account prefix of a physical pool without inferring its membership.
fn valid_pool(pool: &str, account: &str) -> bool {
    pool.strip_prefix(account)
        .and_then(|s| s.strip_prefix("::"))
        .is_some_and(|lane| label(lane, 128))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The peer UID check refuses a real Unix peer before any bytes are sent.
    #[tokio::test]
    async fn quota_client_v1_wrong_peer_receives_no_request() {
        let (client, mut peer) = UnixStream::pair().unwrap();
        // SAFETY: geteuid has no arguments and only reads the process identity.
        let wrong_uid = unsafe { libc::geteuid() } ^ 1;
        assert_eq!(
            authenticate_peer(&client, wrong_uid),
            Err(Error::UnsafeSocket)
        );
        drop(client);
        let mut received = Vec::new();
        peer.read_to_end(&mut received).await.unwrap();
        assert!(received.is_empty());
    }

    /// A single received chunk cannot smuggle a second frame after its newline.
    #[tokio::test]
    async fn quota_client_v1_rejects_buffered_extra_frames() {
        let (mut writer, mut reader) = UnixStream::pair().unwrap();
        writer.write_all(b"{}\n{}\n").await.unwrap();
        assert!(matches!(
            read_response(&mut reader).await,
            Err(Error::InvalidResponse)
        ));
    }
}
