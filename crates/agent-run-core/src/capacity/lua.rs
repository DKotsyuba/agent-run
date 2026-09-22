//! Bounded embedded Lua 5.4 quota collector engine.
//!
//! Every invocation gets a fresh restricted VM: a safe standard-library
//! subset, memory and instruction limits enforced from inside the interpreter
//! (a wall deadline is checked on the same hook, so a tight Lua loop cannot
//! out-wait a socket timeout), and an HTTP capability whose authorization is
//! injected in Rust only for explicitly allowed origins. Scripts never see
//! credential bytes, reference paths, or auth headers. Errors that cross the
//! boundary are static typed categories; Lua error text and HTTP bodies stay
//! inside the VM and are redacted from every diagnostic.

use crate::{
    capacity::quota::{normalize_collector_output, CollectorScope, MAX_OUTPUT_MODELS},
    error::invalid,
    Result,
};
use agent_run_domain::catalog::{AccountId, NormalizedQuotaSnapshot};
use mlua::chunk::{AsChunk, ChunkMode};
use mlua::{HookTriggers, Lua, LuaOptions, LuaSerdeExt, StdLib, Value as LuaValue, VmState};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    fmt,
    future::Future,
    pin::Pin,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

/// Default VM memory bound: 16 MiB per invocation.
pub const DEFAULT_VM_MEMORY_BYTES: usize = 16 * 1024 * 1024;
/// Default instruction budget: 10,000,000 Lua instructions per invocation.
pub const DEFAULT_INSTRUCTIONS: u32 = 10_000_000;
/// Default whole-invocation wall bound.
pub const DEFAULT_INVOCATION_TIMEOUT: Duration = Duration::from_secs(30);
/// Default per-request HTTP budget.
pub const DEFAULT_HTTP_REQUESTS: u32 = 8;
/// Default per-request wall bound.
pub const DEFAULT_HTTP_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
/// Default bounded streamed response body.
pub const DEFAULT_HTTP_BODY_BYTES: usize = 2 * 1024 * 1024;
/// Default and hard maximum output windows per invocation.
pub const DEFAULT_OUTPUT_WINDOWS: usize = 256;

/// Hard ceiling every override is validated against; nothing can be raised past it.
struct HardLimits;

impl HardLimits {
    const MEMORY_BYTES: usize = 64 * 1024 * 1024;
    const INSTRUCTIONS: u32 = 100_000_000;
    const INVOCATION: Duration = Duration::from_secs(600);
    const HTTP_REQUESTS: u32 = 64;
    const HTTP_REQUEST: Duration = Duration::from_secs(60);
    const HTTP_BODY_BYTES: usize = 8 * 1024 * 1024;
}

/// Positive, bounded resource limits for one collector invocation.
///
/// Defaults match the platform contract (16 MiB VM memory, 10M instructions,
/// 30 s invocation, 8 HTTP requests, 10 s and 2 MiB per request, 256 output
/// windows). Overrides must be positive and cannot exceed the hard ceilings.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CollectorLimits {
    /// Interpreter memory cap enforced inside the VM.
    pub vm_memory_bytes: usize,
    /// Instruction budget enforced by the interpreter hook.
    pub instructions: u32,
    /// Whole-invocation wall budget; exceeding it aborts CPU and HTTP work.
    pub invocation_timeout: Duration,
    /// Maximum HTTP requests one invocation may issue, redirects included.
    pub http_requests: u32,
    /// Wall budget for one HTTP request including bounded body streaming.
    pub http_request_timeout: Duration,
    /// Maximum response body bytes delivered to the VM.
    pub http_response_body_bytes: usize,
    /// Maximum windows accepted in the script's output.
    pub output_windows: usize,
}

impl Default for CollectorLimits {
    fn default() -> Self {
        Self {
            vm_memory_bytes: DEFAULT_VM_MEMORY_BYTES,
            instructions: DEFAULT_INSTRUCTIONS,
            invocation_timeout: DEFAULT_INVOCATION_TIMEOUT,
            http_requests: DEFAULT_HTTP_REQUESTS,
            http_request_timeout: DEFAULT_HTTP_REQUEST_TIMEOUT,
            http_response_body_bytes: DEFAULT_HTTP_BODY_BYTES,
            output_windows: DEFAULT_OUTPUT_WINDOWS,
        }
    }
}

impl CollectorLimits {
    /// Rejects zero/absent values and anything above the hard ceilings.
    pub fn validate(&self) -> Result<()> {
        let ok = self.vm_memory_bytes > 0
            && self.vm_memory_bytes <= HardLimits::MEMORY_BYTES
            && self.instructions > 0
            && self.instructions <= HardLimits::INSTRUCTIONS
            && self.invocation_timeout > Duration::ZERO
            && self.invocation_timeout <= HardLimits::INVOCATION
            && self.http_requests > 0
            && self.http_requests <= HardLimits::HTTP_REQUESTS
            && self.http_request_timeout > Duration::ZERO
            && self.http_request_timeout <= HardLimits::HTTP_REQUEST
            && self.http_response_body_bytes > 0
            && self.http_response_body_bytes <= HardLimits::HTTP_BODY_BYTES
            && self.output_windows > 0
            && self.output_windows <= DEFAULT_OUTPUT_WINDOWS;
        ok.then_some(())
            .ok_or_else(|| invalid("invalid collector limits"))
    }
}

/// One exactly configured network origin a collector may request.
///
/// Matching is exact on scheme, ASCII-lowercased host, and port; userinfo,
/// fragments, and any other origin never match.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AllowedOrigin {
    /// Only `https` in production; `http` exists for explicit loopback fixtures.
    pub scheme: String,
    /// Exact host, no wildcards.
    pub host: String,
    /// Explicit port.
    pub port: u16,
}

/// Minimal strict URL parser producing an origin triple, or `None` for any
/// URL with a non-HTTP(S) scheme, userinfo, fragment, or malformed authority.
///
/// Exists so the engine never depends on a URL crate's lenient parsing.
fn parse_origin(url: &str) -> Option<(String, String, u16)> {
    let (scheme, rest) = url.split_once("://")?;
    if !matches!(scheme, "http" | "https") || rest.is_empty() {
        return None;
    }
    if url.contains('#') {
        return None;
    }
    let authority = rest.split(['/', '?']).next().unwrap_or("");
    if authority.is_empty() || authority.contains('@') {
        return None;
    }
    let (host, port) = match authority.rsplit_once(':') {
        Some((host, port)) => (host, port.parse::<u16>().ok()?),
        None => (authority, if scheme == "https" { 443 } else { 80 }),
    };
    if host.is_empty() || port == 0 {
        return None;
    }
    Some((scheme.to_owned(), host.to_ascii_lowercase(), port))
}

impl AllowedOrigin {
    /// Returns whether `url` names exactly this origin and nothing looser.
    pub fn allows(&self, url: &str) -> bool {
        parse_origin(url).is_some_and(|(scheme, host, port)| {
            scheme == self.scheme && host == self.host.to_ascii_lowercase() && port == self.port
        })
    }
}

/// One engine-validated HTTP request, never carrying authorization.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuotaHttpRequest {
    /// `GET` or `POST`.
    pub method: String,
    /// Absolute URL whose origin the engine has already allowlisted.
    pub url: String,
    /// Script-supplied headers, never auth headers.
    pub headers: Vec<(String, String)>,
    /// Optional bounded body bytes.
    pub body: Option<Vec<u8>>,
}

/// One completed HTTP response whose body the engine has already bounded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuotaHttpResponse {
    /// HTTP status code.
    pub status: u16,
    /// Response headers, lowercased names.
    pub headers: Vec<(String, String)>,
    /// Body bytes already within the configured bound.
    pub body: Vec<u8>,
}

/// Static typed HTTP failure categories; none carries remote text.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QuotaHttpError {
    /// URL or redirect target outside the exact allowlist.
    OriginForbidden,
    /// Cross-origin redirect; credentials never follow a redirect off-origin.
    CrossOriginRedirect,
    /// Shared per-invocation request budget exhausted.
    BudgetExhausted,
    /// Per-request wall budget exceeded.
    RequestTimeout,
    /// Response body exceeded the configured bound.
    BodyTooLarge,
    /// Transport failure without any remote detail.
    Transport,
}

impl fmt::Display for QuotaHttpError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            Self::OriginForbidden => "origin_forbidden",
            Self::CrossOriginRedirect => "cross_origin_redirect",
            Self::BudgetExhausted => "budget_exhausted",
            Self::RequestTimeout => "request_timeout",
            Self::BodyTooLarge => "body_too_large",
            Self::Transport => "transport",
        };
        f.write_str(name)
    }
}

/// Injectable HTTP transport for quota collectors.
///
/// Production uses [`ReqwestQuotaHttp`]; tests inject fixtures. The client
/// never sees credentials: the engine attaches authorization only after the
/// origin check succeeds.
pub trait QuotaHttpClient: Send + Sync {
    /// Performs one request; redirects must surface as 3xx responses.
    fn fetch<'a>(
        &'a self,
        request: QuotaHttpRequest,
    ) -> Pin<
        Box<
            dyn Future<Output = std::result::Result<QuotaHttpResponse, QuotaHttpError>> + Send + 'a,
        >,
    >;
}

/// Reqwest-backed production transport: redirects disabled, bodies streamed
/// under `body_limit` bytes, and no cookies or proxies beyond the shared client.
pub struct ReqwestQuotaHttp {
    client: reqwest::Client,
    body_limit: usize,
}

impl ReqwestQuotaHttp {
    /// Builds the transport; fails only when the shared client cannot be built.
    pub fn new(body_limit: usize) -> reqwest::Result<Self> {
        Ok(Self {
            client: reqwest::Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .build()?,
            body_limit,
        })
    }
}

impl QuotaHttpClient for ReqwestQuotaHttp {
    fn fetch<'a>(
        &'a self,
        request: QuotaHttpRequest,
    ) -> Pin<
        Box<
            dyn Future<Output = std::result::Result<QuotaHttpResponse, QuotaHttpError>> + Send + 'a,
        >,
    > {
        Box::pin(async move {
            let method = reqwest::Method::from_bytes(request.method.as_bytes())
                .map_err(|_| QuotaHttpError::Transport)?;
            let mut builder = self.client.request(method, &request.url);
            for (name, value) in &request.headers {
                builder = builder.header(name, value);
            }
            if let Some(body) = request.body {
                builder = builder.body(body);
            }
            let response = builder
                .send()
                .await
                .map_err(|_| QuotaHttpError::Transport)?;
            let status = response.status().as_u16();
            let mut headers = Vec::new();
            for (name, value) in response.headers() {
                headers.push((
                    name.as_str().to_ascii_lowercase(),
                    value.to_str().unwrap_or_default().to_owned(),
                ));
            }
            let mut body = Vec::new();
            let mut response = response;
            while let Some(chunk) = response
                .chunk()
                .await
                .map_err(|_| QuotaHttpError::Transport)?
            {
                if body.len() + chunk.len() > self.body_limit {
                    return Err(QuotaHttpError::BodyTooLarge);
                }
                body.extend_from_slice(&chunk);
            }
            Ok(QuotaHttpResponse {
                status,
                headers,
                body,
            })
        })
    }
}

/// The Rust-held authorization capability for one account.
///
/// The credential value never crosses into Lua; `markers` are the synthetic
/// or real secret substrings scrubbed from any bounded diagnostic the engine
/// could ever emit.
#[derive(Debug, Clone)]
pub struct AuthCapability {
    value: Arc<str>,
    markers: Vec<String>,
}

impl AuthCapability {
    /// Builds the capability; `value` is the raw authorization token the
    /// engine injects as `Authorization` on allowlisted origins.
    pub fn new(value: Arc<str>, markers: Vec<String>) -> Self {
        Self { value, markers }
    }

    /// Returns `text` with every known secret marker replaced.
    pub fn redact(&self, text: &str) -> String {
        let mut out = text.to_owned();
        for marker in self.markers.iter().filter(|m| !m.is_empty()) {
            if out.contains(marker.as_str()) {
                out = out.replace(marker.as_str(), "[redacted]");
            }
        }
        out
    }
}

/// Stable typed failure categories for one collector invocation.
///
/// Variants carry static text only; Lua error strings and HTTP bodies are
/// never embedded, so a secret echoed by a fixture endpoint cannot leak.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CollectorError {
    /// Source failed to compile.
    InvalidScript,
    /// Source or an in-VM `load` call presented Lua bytecode.
    BytecodeRejected,
    /// Instruction budget exceeded.
    InstructionLimit,
    /// VM memory budget exceeded.
    MemoryLimit,
    /// Whole-invocation or per-request wall budget exceeded.
    Timeout,
    /// Script raised its own error; its text is discarded.
    ScriptFailed,
    /// Script hash does not match its exact bytes.
    ScriptTampered,
    /// HTTP failure with its static category.
    Http(QuotaHttpError),
    /// Output was not a `{windows=...}` table.
    MalformedOutput,
    /// Output failed validation; carries the stable domain rejection code.
    InvalidOutput(&'static str),
    /// Engine misuse (bad limits, missing `collect`, internal failure).
    Internal(&'static str),
}

impl fmt::Display for CollectorError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidScript => f.write_str("quota_collector_script_invalid"),
            Self::BytecodeRejected => f.write_str("quota_collector_bytecode_rejected"),
            Self::InstructionLimit => f.write_str("quota_collector_instruction_limit"),
            Self::MemoryLimit => f.write_str("quota_collector_memory_limit"),
            Self::Timeout => f.write_str("quota_collector_timeout"),
            Self::ScriptFailed => f.write_str("quota_collector_script_failed"),
            Self::ScriptTampered => f.write_str("quota_collector_script_tampered"),
            Self::Http(error) => write!(f, "quota_collector_http_{error}"),
            Self::MalformedOutput => f.write_str("quota_collector_output_malformed"),
            Self::InvalidOutput(code) => write!(f, "quota_collector_output_invalid_{code}"),
            Self::Internal(code) => write!(f, "quota_collector_internal_{code}"),
        }
    }
}

/// One collector script with its exact-byte hash frozen per invocation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CollectorScript {
    /// Exact Lua source bytes.
    pub source: String,
    /// Lowercase hex SHA-256 of `source`; a mismatch refuses the invocation.
    pub sha256: String,
}

impl CollectorScript {
    /// Builds a script, hashing the exact source bytes.
    pub fn new(source: impl Into<String>) -> Self {
        let source = source.into();
        let sha256 = hex(&Sha256::digest(source.as_bytes()));
        Self { source, sha256 }
    }

    /// Returns whether the frozen hash still matches the exact bytes and the
    /// source is neither empty, oversized, nor a Lua binary chunk.
    pub fn verify(&self) -> Result<()> {
        let source = self.source.as_bytes();
        if source.is_empty() || source.len() > 256 * 1024 || source.starts_with(b"\x1bLua") {
            return Err(invalid("invalid collector script"));
        }
        if !self
            .sha256
            .eq_ignore_ascii_case(&hex(&Sha256::digest(source)))
        {
            return Err(invalid("collector script hash mismatch"));
        }
        Ok(())
    }
}

/// Lowercase hex encoder kept local to avoid a new dependency.
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// Minimal retained-script registry.
///
/// Installing validates the replacement exactly as an invocation would; a
/// rejected replacement never silently displaces the retained valid revision,
/// and there is no reload machinery beyond explicit reinstallation.
#[derive(Debug, Default)]
pub struct ScriptRegistry {
    scripts: BTreeMap<String, CollectorScript>,
}

impl ScriptRegistry {
    /// Installs `script` under `name` only when it verifies; the previous
    /// revision is retained on any rejection.
    pub fn install(&mut self, name: &str, script: CollectorScript) -> Result<()> {
        script.verify()?;
        self.scripts.insert(name.to_owned(), script);
        Ok(())
    }

    /// Returns the retained script for `name`, if any.
    pub fn get(&self, name: &str) -> Option<&CollectorScript> {
        self.scripts.get(name)
    }
}

/// Sentinel carried by the interpreter hook so aborts map to typed categories.
const INSTRUCTION_SENTINEL: &str = "quota:instruction_limit";
/// Sentinel for the wall deadline checked on the same interpreter hook.
const WALL_SENTINEL: &str = "quota:wall_timeout";
/// Sentinel raised by the HTTP capability, visible to scripts as a category.
const HTTP_SENTINEL: &str = "quota:http:";

/// A source-only chunk: `mode()` pins [`ChunkMode::Text`] so the Lua loader
/// itself rejects bytecode regardless of content sniffing.
struct TextChunk(String);

impl AsChunk for TextChunk {
    fn source<'a>(&self) -> std::io::Result<std::borrow::Cow<'a, [u8]>> {
        Ok(std::borrow::Cow::Owned(self.0.as_bytes().to_vec()))
    }

    fn mode(&self) -> Option<ChunkMode> {
        Some(ChunkMode::Text)
    }
}

/// Shared per-invocation accounting the HTTP capability mutates from Rust.
struct HttpState {
    budget: AtomicU64,
    origins: Vec<AllowedOrigin>,
    limits: CollectorLimits,
    auth_header: (String, String),
}

impl HttpState {
    /// Validates one script-supplied URL against the exact allowlist before
    /// the credential is ever read, then decrements the shared budget.
    fn authorize(&self, url: &str) -> std::result::Result<String, QuotaHttpError> {
        if !self.origins.iter().any(|origin| origin.allows(url)) {
            return Err(QuotaHttpError::OriginForbidden);
        }
        if self.budget.fetch_sub(1, Ordering::SeqCst) == 0 {
            return Err(QuotaHttpError::BudgetExhausted);
        }
        Ok(url.to_owned())
    }
}

/// Runs one collector invocation in a fresh restricted VM.
///
/// `scope` supplies the persistence runtime, the collector source identity
/// (never script-chosen), and the configured models; `models` carries each
/// model's nonsecret configuration value into `ctx.models`. The script must
/// define `collect(ctx)` and return `{windows = {...}}` in version-1 shape;
/// the result is normalized and validated against `scope` before returning.
/// Fresh failures are typed [`CollectorError`] categories with static text.
pub async fn run_collector(
    script: &CollectorScript,
    scope: &CollectorScope,
    account: &AccountId,
    models: &BTreeMap<String, Value>,
    host_now: f64,
    limits: &CollectorLimits,
    origins: &[AllowedOrigin],
    client: Arc<dyn QuotaHttpClient>,
    auth: &AuthCapability,
) -> std::result::Result<NormalizedQuotaSnapshot, CollectorError> {
    if script.source.as_bytes().starts_with(b"\x1bLua") {
        return Err(CollectorError::BytecodeRejected);
    }
    script
        .verify()
        .map_err(|_| CollectorError::ScriptTampered)?;
    limits
        .validate()
        .map_err(|_| CollectorError::Internal("limits"))?;
    if models.keys().any(|model| !scope.models.contains(model)) || !host_now.is_finite() {
        return Err(CollectorError::Internal("context"));
    }
    let deadline = Instant::now() + limits.invocation_timeout;
    let work = invoke(
        script, scope, account, models, host_now, limits, origins, client, auth, deadline,
    );
    match tokio::time::timeout_at(deadline.into(), work).await {
        Ok(result) => result,
        Err(_) => Err(CollectorError::Timeout),
    }
}

/// One invocation's VM lifecycle; boxed inner future so the outer timeout can
/// cancel (drop) it, which joins all interpreter and HTTP work on this task.
async fn invoke(
    script: &CollectorScript,
    scope: &CollectorScope,
    account: &AccountId,
    models: &BTreeMap<String, Value>,
    host_now: f64,
    limits: &CollectorLimits,
    origins: &[AllowedOrigin],
    client: Arc<dyn QuotaHttpClient>,
    auth: &AuthCapability,
    deadline: Instant,
) -> std::result::Result<NormalizedQuotaSnapshot, CollectorError> {
    let lua = Lua::new_with(
        StdLib::COROUTINE | StdLib::TABLE | StdLib::STRING | StdLib::MATH | StdLib::UTF8,
        LuaOptions::new(),
    )
    .map_err(|_| CollectorError::Internal("vm"))?;
    lua.set_memory_limit(limits.vm_memory_bytes)
        .map_err(|_| CollectorError::Internal("memory_limit"))?;
    let hook_instructions = Arc::new(AtomicU64::new(0));
    let instruction_budget = limits.instructions;
    lua.set_global_hook(
        HookTriggers::new().every_nth_instruction(2_000),
        move |_, _| {
            let executed = hook_instructions.fetch_add(2_000, Ordering::Relaxed) + 2_000;
            if executed > u64::from(instruction_budget) {
                return Err(mlua::Error::RuntimeError(INSTRUCTION_SENTINEL.into()));
            }
            if Instant::now() > deadline {
                return Err(mlua::Error::RuntimeError(WALL_SENTINEL.into()));
            }
            Ok(VmState::Continue)
        },
    )
    .map_err(|_| CollectorError::Internal("hook"))?;
    let globals = lua.globals();
    for escape in ["load", "loadstring", "dofile", "require", "print"] {
        globals
            .set(escape, mlua::Nil)
            .map_err(|_| CollectorError::Internal("sandbox"))?;
    }

    let request_state = Arc::new(HttpState {
        budget: AtomicU64::new(u64::from(limits.http_requests)),
        origins: origins.to_vec(),
        limits: *limits,
        auth_header: ("authorization".into(), auth.value.to_string()),
    });
    let request = lua
        .create_async_function(move |lua, table: LuaValue| {
            let client = client.clone();
            let state = request_state.clone();
            async move {
                let table = match table {
                    LuaValue::Table(table) => table,
                    _ => {
                        return Err(mlua::Error::RuntimeError(format!(
                            "{HTTP_SENTINEL}origin_forbidden"
                        )))
                    }
                };
                let url: String = table.get("url").map_err(|_| {
                    mlua::Error::RuntimeError(format!("{HTTP_SENTINEL}origin_forbidden"))
                })?;
                let authorized = state.authorize(&url).map_err(|error| {
                    mlua::Error::RuntimeError(format!("{HTTP_SENTINEL}{error}"))
                })?;
                let method: Option<String> = table.get("method").ok();
                let method = method.unwrap_or_else(|| "GET".into());
                if !matches!(method.as_str(), "GET" | "POST" | "HEAD") {
                    return Err(mlua::Error::RuntimeError(format!(
                        "{HTTP_SENTINEL}origin_forbidden"
                    )));
                }
                let mut headers: Vec<(String, String)> = Vec::new();
                if let Ok(header_table) = table.get::<mlua::Table>("headers") {
                    for pair in header_table.pairs::<String, String>() {
                        let (name, value) = pair.map_err(|_| {
                            mlua::Error::RuntimeError(format!("{HTTP_SENTINEL}origin_forbidden"))
                        })?;
                        if name.eq_ignore_ascii_case("authorization") {
                            return Err(mlua::Error::RuntimeError(format!(
                                "{HTTP_SENTINEL}origin_forbidden"
                            )));
                        }
                        headers.push((name, value));
                    }
                }
                let body: Option<String> = table.get("body").ok();
                let body = body.map(|b| b.into_bytes());
                let mut remaining_redirects = 4;
                let mut current = authorized.clone();
                loop {
                    let mut request = QuotaHttpRequest {
                        method: method.clone(),
                        url: current.clone(),
                        headers: headers.clone(),
                        body: body.clone(),
                    };
                    // Credential injection happens here, in Rust, only after
                    // the origin check above has already passed.
                    request.headers.push(state.auth_header.clone());
                    let response = tokio::time::timeout(
                        state.limits.http_request_timeout,
                        client.fetch(request),
                    )
                    .await
                    .map_err(|_| {
                        mlua::Error::RuntimeError(format!("{HTTP_SENTINEL}request_timeout"))
                    })?
                    .map_err(|error| {
                        mlua::Error::RuntimeError(format!("{HTTP_SENTINEL}{error}"))
                    })?;
                    let location = if (301..=308).contains(&response.status) {
                        response
                            .headers
                            .iter()
                            .find(|(name, _)| name == "location")
                            .map(|(_, value)| value.clone())
                    } else {
                        None
                    };
                    if let Some(location) = location {
                        if remaining_redirects == 0 {
                            return Err(mlua::Error::RuntimeError(format!(
                                "{HTTP_SENTINEL}budget_exhausted"
                            )));
                        }
                        remaining_redirects -= 1;
                        let next = resolve_redirect(&current, &location).ok_or_else(|| {
                            mlua::Error::RuntimeError(format!(
                                "{HTTP_SENTINEL}cross_origin_redirect"
                            ))
                        })?;
                        // No cross-origin credential redirects: the redirect
                        // target must name the exact origin first requested.
                        if parse_origin(&next) != parse_origin(&authorized) {
                            return Err(mlua::Error::RuntimeError(format!(
                                "{HTTP_SENTINEL}cross_origin_redirect"
                            )));
                        }
                        current = next;
                        continue;
                    }
                    if response.body.len() > state.limits.http_response_body_bytes {
                        return Err(mlua::Error::RuntimeError(format!(
                            "{HTTP_SENTINEL}body_too_large"
                        )));
                    }
                    let out = lua.create_table().map_err(|_| {
                        mlua::Error::RuntimeError(format!("{HTTP_SENTINEL}transport"))
                    })?;
                    out.set("status", response.status).map_err(|_| {
                        mlua::Error::RuntimeError(format!("{HTTP_SENTINEL}transport"))
                    })?;
                    out.set(
                        "body",
                        lua.create_string(&response.body).map_err(|_| {
                            mlua::Error::RuntimeError(format!("{HTTP_SENTINEL}transport"))
                        })?,
                    )
                    .map_err(|_| mlua::Error::RuntimeError(format!("{HTTP_SENTINEL}transport")))?;
                    let header_table = lua.create_table().map_err(|_| {
                        mlua::Error::RuntimeError(format!("{HTTP_SENTINEL}transport"))
                    })?;
                    for (name, value) in response.headers {
                        header_table.set(name, value).map_err(|_| {
                            mlua::Error::RuntimeError(format!("{HTTP_SENTINEL}transport"))
                        })?;
                    }
                    out.set("headers", header_table).map_err(|_| {
                        mlua::Error::RuntimeError(format!("{HTTP_SENTINEL}transport"))
                    })?;
                    return Ok(out);
                }
            }
        })
        .map_err(|_| CollectorError::Internal("http"))?;
    let ctx = lua
        .create_table()
        .map_err(|_| CollectorError::Internal("context"))?;
    let account_table = lua
        .create_table()
        .map_err(|_| CollectorError::Internal("context"))?;
    account_table
        .set("id", account.as_str())
        .map_err(|_| CollectorError::Internal("context"))?;
    ctx.set("account", account_table)
        .map_err(|_| CollectorError::Internal("context"))?;
    let models_table = lua
        .create_table()
        .map_err(|_| CollectorError::Internal("context"))?;
    for (model, config) in models {
        let value = lua
            .to_value(config)
            .map_err(|_| CollectorError::Internal("context"))?;
        models_table
            .set(model.clone(), value)
            .map_err(|_| CollectorError::Internal("context"))?;
    }
    ctx.set("models", models_table)
        .map_err(|_| CollectorError::Internal("context"))?;
    ctx.set("now", host_now)
        .map_err(|_| CollectorError::Internal("context"))?;
    // Opaque capability marker: no method, no secret, nothing to extract.
    let auth_marker = lua
        .create_table()
        .map_err(|_| CollectorError::Internal("context"))?;
    ctx.set("auth", auth_marker)
        .map_err(|_| CollectorError::Internal("context"))?;
    let http_table = lua
        .create_table()
        .map_err(|_| CollectorError::Internal("context"))?;
    http_table
        .set("request", request)
        .map_err(|_| CollectorError::Internal("context"))?;
    ctx.set("http", http_table)
        .map_err(|_| CollectorError::Internal("context"))?;

    let function = lua
        .load(TextChunk(script.source.clone()))
        .set_name("collector")
        .into_function()
        .map_err(map_lua_error)?;
    function.call_async::<()>(()).await.map_err(map_lua_error)?;
    let collect: mlua::Function = globals
        .get("collect")
        .map_err(|_| CollectorError::Internal("missing_collect"))?;
    let output = collect
        .call_async::<LuaValue>(ctx)
        .await
        .map_err(map_lua_error)?;
    let json = lua
        .from_value::<Value>(output)
        .map_err(|_| CollectorError::MalformedOutput)?;
    let snapshot = normalize_collector_output(
        account,
        scope,
        &json,
        limits.output_windows,
        MAX_OUTPUT_MODELS,
    )
    .map_err(|error| match error {
        crate::Error::Validation(code) => CollectorError::InvalidOutput(leak_static(code)),
        _ => CollectorError::MalformedOutput,
    })?;
    Ok(snapshot)
}

/// Resolves a `Location` value against the requested URL, rejecting anything
/// but a same-origin absolute or path-absolute target.
fn resolve_redirect(current: &str, location: &str) -> Option<String> {
    let (scheme, host, port) = parse_origin(current)?;
    if location.starts_with("http://") || location.starts_with("https://") {
        let (ls, lh, lp) = parse_origin(location)?;
        return (ls == scheme && lh == host && lp == port).then(|| location.to_owned());
    }
    if location.starts_with('/') && !location.contains("://") {
        return Some(format!("{scheme}://{host}:{port}{location}"));
    }
    None
}

/// Maps an interpreter failure to its typed category, discarding all text
/// except the engine's own sentinels.
fn map_lua_error(error: mlua::Error) -> CollectorError {
    if let mlua::Error::SyntaxError { .. } = error {
        return CollectorError::InvalidScript;
    }
    if matches!(error, mlua::Error::MemoryError(_)) {
        return CollectorError::MemoryLimit;
    }
    let text = error.to_string();
    if text.contains(INSTRUCTION_SENTINEL) {
        CollectorError::InstructionLimit
    } else if text.contains(WALL_SENTINEL) {
        CollectorError::Timeout
    } else if let Some(category) = text
        .split(HTTP_SENTINEL)
        .nth(1)
        .and_then(|rest| {
            rest.split(|c: char| c.is_whitespace() || c == ':' || c == '"')
                .next()
        })
        .and_then(parse_http_category)
    {
        CollectorError::Http(category)
    } else if text.contains("binary") || text.contains("bytecode") {
        CollectorError::BytecodeRejected
    } else {
        CollectorError::ScriptFailed
    }
}

/// Converts a static HTTP category name back from a sentinel message.
fn parse_http_category(name: &str) -> Option<QuotaHttpError> {
    match name {
        "origin_forbidden" => Some(QuotaHttpError::OriginForbidden),
        "cross_origin_redirect" => Some(QuotaHttpError::CrossOriginRedirect),
        "budget_exhausted" => Some(QuotaHttpError::BudgetExhausted),
        "request_timeout" => Some(QuotaHttpError::RequestTimeout),
        "body_too_large" => Some(QuotaHttpError::BodyTooLarge),
        "transport" => Some(QuotaHttpError::Transport),
        _ => None,
    }
}

/// Keeps only the static rejection codes the normalizer itself defines, so no
/// foreign text can ride along in an external error.
fn leak_static(code: String) -> &'static str {
    match code.as_str() {
        "quota_output_malformed" => "quota_output_malformed",
        "quota_output_overflow" => "quota_output_overflow",
        "quota_output_foreign_model" => "quota_output_foreign_model",
        "quota_output_duplicate_window" => "quota_output_duplicate_window",
        "quota_output_invalid_number" => "quota_output_invalid_number",
        "quota_output_invalid_time" => "quota_output_invalid_time",
        "too many quota models" => "too_many_quota_models",
        "invalid quota model membership" => "invalid_quota_model_membership",
        "invalid quota window" => "invalid_quota_window",
        "quota key belongs to another account or repeats" => "quota_key_foreign",
        "shared quota pool observations disagree" => "shared_pool_disagreement",
        "quota lane exceeds 128 bytes" => "quota_lane_overflow",
        _ => "output_invalid",
    }
}
