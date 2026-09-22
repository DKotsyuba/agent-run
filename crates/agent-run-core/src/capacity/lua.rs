//! Bounded embedded Lua 5.4 quota collector engine.
//!
//! Every invocation gets a fresh base-only VM with memory and instruction
//! limits enforced inside the interpreter and a wall deadline checked by its
//! hook. An HTTP capability injects authorization in Rust only for allowed
//! origins. Scripts never see
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
use mlua::{
    HookTriggers, Lua, LuaOptions, LuaSerdeExt, MultiValue, StdLib, Value as LuaValue, VmState,
};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    fmt,
    future::Future,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
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
    /// Whole-invocation wall budget checked at Lua hooks and async boundaries;
    /// bounded native host calls cannot be preempted mid-call.
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
/// Matching uses the same canonical `reqwest::Url` interpretation as the
/// transport, so a URL can never pass one parser and be reinterpreted by
/// another. HTTPS is required; plain HTTP exists only for explicit loopback
/// fixtures. Userinfo, fragments, non-HTTP(S) schemes, and any looser or
/// wildcard spelling never match.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AllowedOrigin {
    /// `https`, or `http` only for an explicit loopback fixture.
    pub scheme: String,
    /// Exact host, no wildcards.
    pub host: String,
    /// Explicit port compared against the URL's effective port.
    pub port: u16,
}

impl AllowedOrigin {
    /// Validates one origin: HTTPS for production, plain HTTP only for
    /// loopback fixtures (`localhost`, `127.0.0.1`, `::1`).
    pub fn new(scheme: &str, host: &str, port: u16) -> Result<Self> {
        let host = host
            .trim_start_matches('[')
            .trim_end_matches(']')
            .to_ascii_lowercase();
        if host.is_empty() || host.len() > 253 || port == 0 {
            return Err(invalid("invalid collector origin"));
        }
        match scheme {
            "https" => {}
            "http" if matches!(host.as_str(), "localhost" | "127.0.0.1" | "::1") => {}
            _ => return Err(invalid("collector origin must be https, or loopback http")),
        }
        Ok(Self {
            scheme: scheme.into(),
            host,
            port,
        })
    }

    /// Returns the canonical `(scheme, host, effective port)` of `url` using
    /// `reqwest::Url`, or `None` when it carries userinfo, a fragment, a
    /// non-HTTP(S) scheme, or no host.
    pub fn canonical(url: &str) -> Option<(String, String, u16)> {
        let url = reqwest::Url::parse(url).ok()?;
        if !matches!(url.scheme(), "http" | "https")
            || !url.username().is_empty()
            || url.password().is_some()
            || url.fragment().is_some()
        {
            return None;
        }
        let host = url
            .host_str()?
            .trim_start_matches('[')
            .trim_end_matches(']')
            .to_ascii_lowercase();
        let port = url.port_or_known_default()?;
        (!host.is_empty()).then(|| (url.scheme().to_owned(), host, port))
    }

    /// Returns whether `url` names exactly this origin under the canonical
    /// transport interpretation.
    pub fn allows(&self, url: &str) -> bool {
        Self::canonical(url).is_some_and(|(scheme, host, port)| {
            scheme == self.scheme && host == self.host && port == self.port
        })
    }
}

/// One engine-validated HTTP request as handed to the transport.
///
/// The engine injects account authorization into `headers` after the origin
/// check, so the derived [`fmt::Debug`] redacts every header value and body;
/// no `{:?}` output of this type can carry credential bytes.
#[derive(Clone, PartialEq, Eq)]
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

impl fmt::Debug for QuotaHttpRequest {
    /// Redacts all header values and the body: this is the one type through
    /// which injected credential material could otherwise reach a log.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("QuotaHttpRequest")
            .field("method", &self.method)
            .field("url", &self.url)
            .field(
                "headers",
                &self
                    .headers
                    .iter()
                    .map(|(name, _)| (name, "[redacted]"))
                    .collect::<Vec<_>>(),
            )
            .field("body", &self.body.as_ref().map(|_| "[redacted]"))
            .finish()
    }
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
    /// Script tried to set an auth or routing-control header; placement is
    /// Rust-owned configuration.
    HeaderForbidden,
    /// Endpoint signalled throttling (429/503); carries the parsed bounded
    /// `retry-after` horizon in seconds when the endpoint supplied one.
    RateLimited,
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
            Self::HeaderForbidden => "header_forbidden",
            Self::RateLimited => "rate_limited",
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

/// Where Rust places the account credential on an allowed request.
///
/// Placement is explicit Rust configuration per provider contract: GLM quota
/// uses a raw `Authorization` token (unlike its Bearer inference gateway),
/// Claude-style OAuth uses `Bearer`, and gateway API keys use a named header.
/// Lua never sees or overrides it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthPlacement {
    /// `Authorization: <token>` exactly as stored.
    RawAuthorization,
    /// `Authorization: Bearer <token>`.
    BearerAuthorization,
    /// A named request header carrying the token (gateway API-key style).
    Header(String),
}

impl AuthPlacement {
    /// The exact header pair the engine injects after the origin check.
    fn header(&self, value: &str) -> (String, String) {
        match self {
            Self::RawAuthorization => ("authorization".into(), value.to_owned()),
            Self::BearerAuthorization => ("authorization".into(), format!("Bearer {value}")),
            Self::Header(name) => (name.to_ascii_lowercase(), value.to_owned()),
        }
    }
}

/// The Rust-held authorization capability for exactly one account and its
/// declared credential origins.
///
/// The capability is constructed bound to the selected global account and the
/// validated origins it may authenticate against, so it cannot be paired with
/// a different account or widen its origin set at invocation time. The
/// credential value never crosses into Lua; the value itself is always a
/// redaction marker, so scrubbing never depends solely on a caller-supplied
/// marker list. The derived [`fmt::Debug`] is redacted.
pub struct AuthCapability {
    account: AccountId,
    placement: AuthPlacement,
    value: Arc<str>,
    markers: Vec<String>,
    origins: Vec<AllowedOrigin>,
}

impl fmt::Debug for AuthCapability {
    /// Shows binding facts only; the credential value and marker contents
    /// never appear in derived debug output.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AuthCapability")
            .field("account", &self.account)
            .field("placement", &self.placement)
            .field("origins", &self.origins)
            .field("markers", &self.markers.len())
            .finish_non_exhaustive()
    }
}

impl AuthCapability {
    /// Builds the capability for `account`, placing `value` per `placement`
    /// on exactly `origins`; `origins` must be nonempty and each origin must
    /// validate (HTTPS, or explicit loopback HTTP). The value itself is
    /// registered as a redaction marker automatically.
    pub fn new(
        account: AccountId,
        placement: AuthPlacement,
        value: Arc<str>,
        origins: Vec<AllowedOrigin>,
    ) -> Result<Self> {
        if origins.is_empty() || value.is_empty() {
            return Err(invalid("capability needs a value and at least one origin"));
        }
        for origin in &origins {
            AllowedOrigin::new(&origin.scheme, &origin.host, origin.port)?;
        }
        Ok(Self {
            account,
            placement,
            markers: vec![value.to_string()],
            value,
            origins,
        })
    }

    /// The global account this capability is bound to.
    pub fn account(&self) -> &AccountId {
        &self.account
    }

    /// The exact origins this capability may authenticate against.
    pub fn origins(&self) -> &[AllowedOrigin] {
        &self.origins
    }

    /// The configured credential header pair the engine injects.
    fn header_pair(&self) -> (String, String) {
        self.placement.header(&self.value)
    }

    /// Returns `text` with the credential value and every extra marker
    /// replaced.
    pub fn redact(&self, text: &str) -> String {
        let mut out = text.to_owned();
        for marker in self.secret_material() {
            if !marker.is_empty() && out.contains(&marker) {
                out = out.replace(&marker, "[redacted]");
            }
        }
        out
    }

    /// The credential value first, then any additional markers.
    fn secret_material(&self) -> Vec<String> {
        let mut material = vec![self.value.to_string()];
        material.extend(self.markers.iter().cloned());
        material
    }
}

/// Replaces every occurrence of `needle` in `haystack` with `replacement`.
fn replace_bytes(haystack: &[u8], needle: &[u8], replacement: &[u8]) -> Vec<u8> {
    if needle.is_empty() || haystack.len() < needle.len() {
        return haystack.to_vec();
    }
    let mut out = Vec::with_capacity(haystack.len());
    let mut index = 0;
    while index < haystack.len() {
        if haystack[index..].starts_with(needle) {
            out.extend_from_slice(replacement);
            index += needle.len();
        } else {
            out.push(haystack[index]);
            index += 1;
        }
    }
    out
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
    /// Endpoint throttled the round (429/503); carries the parsed bounded
    /// `retry-after` horizon in seconds when the endpoint supplied one.
    RateLimited(Option<u64>),
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
            Self::RateLimited(_) => f.write_str("quota_collector_rate_limited"),
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

/// Compiles `source` in text mode inside a bounded throwaway VM.
///
/// Compilation only: nothing executes, so a compile check has no top-level
/// side effects, no network capability, and no `collect` call. Syntax-invalid
/// sources and invalid limits are rejected here, before any invocation or
/// registry replacement; source bytes may not exceed the VM memory cap;
/// the presence and contract of `collect(ctx)` is enforced per invocation.
pub fn compile_check(source: &str, limits: &CollectorLimits) -> Result<()> {
    limits.validate()?;
    if source.len() > limits.vm_memory_bytes {
        return Err(invalid("collector script exceeds vm memory limit"));
    }
    let lua = Lua::new_with(StdLib::NONE, LuaOptions::new())
        .map_err(|_| invalid("collector vm unavailable"))?;
    lua.set_memory_limit(limits.vm_memory_bytes)
        .map_err(|_| invalid("collector vm unavailable"))?;
    lua.load(TextChunk(source.to_owned()))
        .set_name("collector")
        .into_function()
        .map_err(|_| invalid("collector script does not compile"))?;
    Ok(())
}

impl ScriptRegistry {
    /// Installs `script` under `name` only when it verifies and compiles in
    /// text mode within validated limits and the source-size cap; the previous
    /// revision is retained on any rejection.
    pub fn install(
        &mut self,
        name: &str,
        script: CollectorScript,
        limits: &CollectorLimits,
    ) -> Result<()> {
        script.verify()?;
        compile_check(&script.source, limits)?;
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
    /// Credential material scrubbed from every response before Lua exposure.
    secrets: Vec<String>,
}

impl HttpState {
    /// Header names scripts may never set: the configured credential header
    /// plus routing-control headers that could redirect or re-authenticate.
    fn forbidden_header(&self, name: &str) -> bool {
        let name = name.to_ascii_lowercase();
        name == self.auth_header.0
            || matches!(
                name.as_str(),
                "authorization" | "proxy-authorization" | "host" | "cookie" | "connection"
            )
    }

    /// Validates one URL against the exact allowlist before the credential is
    /// ever read, then atomically consumes one unit of the shared budget with
    /// a checked compare-and-swap: an exhausted budget stays exhausted and
    /// can never underflow or be retried past zero by a caught error.
    fn authorize(&self, url: &str) -> std::result::Result<String, QuotaHttpError> {
        if !self.origins.iter().any(|origin| origin.allows(url)) {
            return Err(QuotaHttpError::OriginForbidden);
        }
        let mut current = self.budget.load(Ordering::SeqCst);
        loop {
            if current == 0 {
                return Err(QuotaHttpError::BudgetExhausted);
            }
            match self.budget.compare_exchange(
                current,
                current - 1,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => break,
                Err(observed) => current = observed,
            }
        }
        Ok(url.to_owned())
    }

    /// Scrubs credential material from response bytes and header values
    /// before anything is exposed to Lua.
    fn scrub(&self, bytes: &[u8]) -> Vec<u8> {
        let mut out = bytes.to_vec();
        for secret in &self.secrets {
            if !secret.is_empty() {
                out = replace_bytes(&out, secret.as_bytes(), b"[redacted]");
            }
        }
        out
    }

    /// Returns the bounded numeric `retry-after` seconds a throttled
    /// response declared, or `None` when absent, unparseable, or over the
    /// hard ceiling. Header names are already lowercased.
    fn retry_after_seconds(response: &QuotaHttpResponse) -> Option<u64> {
        let raw = response
            .headers
            .iter()
            .find(|(name, _)| name == "retry-after")
            .map(|(_, value)| value.trim())?;
        let seconds: u64 = raw.parse().ok()?;
        (seconds > 0 && seconds <= 900).then_some(seconds)
    }

    /// Classifies one completed response: throttled statuses become the
    /// typed rate-limit category — with the parsed horizon carried in the
    /// static sentinel text, never raw headers — instead of a
    /// script-visible response table.
    fn throttle(response: &QuotaHttpResponse) -> Option<mlua::Error> {
        matches!(response.status, 429 | 503).then(|| {
            let horizon = Self::retry_after_seconds(response);
            mlua::Error::RuntimeError(format!(
                "{HTTP_SENTINEL}rate_limited:{}",
                horizon
                    .map(|seconds| seconds.to_string())
                    .unwrap_or_default()
            ))
        })
    }
}

/// Runs one collector invocation in a fresh restricted VM.
///
/// `scope` supplies the persistence runtime, the collector source identity
/// (never script-chosen), and the configured models; `models` carries each
/// model's nonsecret configuration value into `ctx.models`; `origin` is the
/// single Rust-chosen request origin exposed as `ctx.origin`, which must name
/// exactly one of `auth`'s validated origins so a script never discovers or
/// widens the credential allowlist. The script must
/// define `collect(ctx)` and return `{windows = {...}}` in version-1 shape;
/// the result is normalized and validated against `scope` before returning.
/// Fresh failures are typed [`CollectorError`] categories with static text.
/// The VM exposes base Lua only; hook deadlines cannot preempt native host
/// calls mid-call, so the wall timeout is cooperative rather than real-time.
pub async fn run_collector(
    script: &CollectorScript,
    scope: &CollectorScope,
    account: &AccountId,
    models: &BTreeMap<String, Value>,
    host_now: f64,
    limits: &CollectorLimits,
    client: Arc<dyn QuotaHttpClient>,
    auth: &AuthCapability,
    origin: &str,
) -> std::result::Result<NormalizedQuotaSnapshot, CollectorError> {
    if script.source.as_bytes().starts_with(b"\x1bLua") {
        return Err(CollectorError::BytecodeRejected);
    }
    limits
        .validate()
        .map_err(|_| CollectorError::Internal("limits"))?;
    if script.source.len() > limits.vm_memory_bytes {
        return Err(CollectorError::MemoryLimit);
    }
    script
        .verify()
        .map_err(|_| CollectorError::ScriptTampered)?;
    if models.keys().any(|model| !scope.models.contains(model)) || !host_now.is_finite() {
        return Err(CollectorError::Internal("context"));
    }
    // The capability is bound to exactly this account and its own validated
    // origins; it cannot be paired with another account or widened here.
    if auth.account() != account {
        return Err(CollectorError::Internal("auth_account"));
    }
    for origin in auth.origins() {
        AllowedOrigin::new(&origin.scheme, &origin.host, origin.port)
            .map_err(|_| CollectorError::Internal("origins"))?;
    }
    if !auth.origins().iter().any(|allowed| allowed.allows(origin)) {
        return Err(CollectorError::Internal("origin"));
    }
    // Scripts concatenate paths onto `ctx.origin`; Lua has no string library,
    // so Rust normalizes the trailing slash they would otherwise duplicate.
    let origin = origin.trim_end_matches('/');
    let deadline = Instant::now() + limits.invocation_timeout;
    let work = invoke(
        script, scope, account, models, host_now, limits, client, auth, origin, deadline,
    );
    match tokio::time::timeout_at(deadline.into(), work).await {
        Ok(result) => result,
        Err(_) => Err(CollectorError::Timeout),
    }
}

/// One invocation's base-only VM lifecycle; the outer timeout can cancel
/// pending HTTP work, while synchronous Lua work checks its hook deadline.
async fn invoke(
    script: &CollectorScript,
    scope: &CollectorScope,
    account: &AccountId,
    models: &BTreeMap<String, Value>,
    host_now: f64,
    limits: &CollectorLimits,
    client: Arc<dyn QuotaHttpClient>,
    auth: &AuthCapability,
    origin: &str,
    deadline: Instant,
) -> std::result::Result<NormalizedQuotaSnapshot, CollectorError> {
    let lua = Lua::new_with(StdLib::NONE, LuaOptions::new())
        .map_err(|_| CollectorError::Internal("vm"))?;
    lua.set_memory_limit(limits.vm_memory_bytes)
        .map_err(|_| CollectorError::Internal("memory_limit"))?;
    let hook_instructions = Arc::new(AtomicU64::new(0));
    let instruction_budget = limits.instructions;
    // Set once the wall deadline passes; every later hook tick errors and the
    // guarded pcall/xpcall wrappers refuse, so a script that catches abort
    // errors inside pcall still collapses within bounded hook
    // ticks and the invocation terminates on its own.
    let deadline_exceeded = Arc::new(AtomicBool::new(false));
    let hook_latch = deadline_exceeded.clone();
    lua.set_global_hook(
        HookTriggers::new().every_nth_instruction(2_000),
        move |_, _| {
            let executed = hook_instructions.fetch_add(2_000, Ordering::Relaxed) + 2_000;
            if executed > u64::from(instruction_budget) {
                hook_latch.store(true, Ordering::SeqCst);
                return Err(mlua::Error::RuntimeError(INSTRUCTION_SENTINEL.into()));
            }
            if Instant::now() > deadline {
                hook_latch.store(true, Ordering::SeqCst);
                return Err(mlua::Error::RuntimeError(WALL_SENTINEL.into()));
            }
            Ok(VmState::Continue)
        },
    )
    .map_err(|_| CollectorError::Internal("hook"))?;
    let globals = lua.globals();
    for escape in [
        "load",
        "loadfile",
        "loadstring",
        "dofile",
        "require",
        "print",
        "collectgarbage",
    ] {
        globals
            .set(escape, mlua::Nil)
            .map_err(|_| CollectorError::Internal("sandbox"))?;
    }
    // Guarded pcall/xpcall: identical to the originals, except that once the
    // instruction or wall budget is exhausted they refuse to protect
    // anything, so no Lua catch loop can outlive the invocation. Hook errors
    // are ordinary catchable Lua 5.4 errors; optional coroutine libraries are
    // absent, so they cannot create another protected execution path.
    for name in ["pcall", "xpcall"] {
        let real: mlua::Function = globals
            .get(name)
            .map_err(|_| CollectorError::Internal("sandbox"))?;
        let latch = deadline_exceeded.clone();
        let guarded = lua
            .create_function(move |_lua, args: MultiValue| {
                if latch.load(Ordering::SeqCst) {
                    return Err(mlua::Error::RuntimeError(WALL_SENTINEL.into()));
                }
                real.call::<MultiValue>(args)
            })
            .map_err(|_| CollectorError::Internal("sandbox"))?;
        globals
            .set(name, guarded)
            .map_err(|_| CollectorError::Internal("sandbox"))?;
    }

    let request_state = Arc::new(HttpState {
        budget: AtomicU64::new(u64::from(limits.http_requests)),
        origins: auth.origins().to_vec(),
        limits: *limits,
        auth_header: auth.header_pair(),
        secrets: auth.secret_material(),
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
                        if state.forbidden_header(&name) {
                            return Err(mlua::Error::RuntimeError(format!(
                                "{HTTP_SENTINEL}header_forbidden"
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
                    // the origin check and budget accounting have passed.
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
                    // Throttled responses never reach Lua: the typed category
                    // with its parsed horizon is the only observable fact.
                    if let Some(error) = HttpState::throttle(&response) {
                        return Err(error);
                    }
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
                        // Resolve with the transport's own URL semantics.
                        let next = reqwest::Url::parse(&current)
                            .ok()
                            .and_then(|base| base.join(&location).ok())
                            .map(|url| url.to_string())
                            .ok_or_else(|| {
                                mlua::Error::RuntimeError(format!(
                                    "{HTTP_SENTINEL}cross_origin_redirect"
                                ))
                            })?;
                        // No cross-origin credential redirects: the target
                        // must name the exact origin first requested, and
                        // every hop re-authorizes against origin AND budget.
                        if AllowedOrigin::canonical(&next) != AllowedOrigin::canonical(&authorized)
                        {
                            return Err(mlua::Error::RuntimeError(format!(
                                "{HTTP_SENTINEL}cross_origin_redirect"
                            )));
                        }
                        state.authorize(&next).map_err(|error| {
                            mlua::Error::RuntimeError(format!("{HTTP_SENTINEL}{error}"))
                        })?;
                        current = next;
                        continue;
                    }
                    if response.body.len() > state.limits.http_response_body_bytes {
                        return Err(mlua::Error::RuntimeError(format!(
                            "{HTTP_SENTINEL}body_too_large"
                        )));
                    }
                    // Scrub credential material from body and headers before
                    // anything is exposed to Lua, regardless of markers a
                    // caller happened to supply.
                    let scrubbed_body = state.scrub(&response.body);
                    let out = lua.create_table().map_err(|_| {
                        mlua::Error::RuntimeError(format!("{HTTP_SENTINEL}transport"))
                    })?;
                    out.set("status", response.status).map_err(|_| {
                        mlua::Error::RuntimeError(format!("{HTTP_SENTINEL}transport"))
                    })?;
                    out.set(
                        "body",
                        lua.create_string(&scrubbed_body).map_err(|_| {
                            mlua::Error::RuntimeError(format!("{HTTP_SENTINEL}transport"))
                        })?,
                    )
                    .map_err(|_| mlua::Error::RuntimeError(format!("{HTTP_SENTINEL}transport")))?;
                    let header_table = lua.create_table().map_err(|_| {
                        mlua::Error::RuntimeError(format!("{HTTP_SENTINEL}transport"))
                    })?;
                    for (name, value) in &response.headers {
                        header_table
                            .set(
                                name.clone(),
                                String::from_utf8_lossy(&state.scrub(value.as_bytes()))
                                    .into_owned(),
                            )
                            .map_err(|_| {
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
    // mlua loads the coroutine library while registering any async callback;
    // remove its Lua-facing entry point after ctx.http.request is installed.
    globals
        .set("coroutine", mlua::Nil)
        .map_err(|_| CollectorError::Internal("sandbox"))?;
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
    // The one Rust-chosen request origin; the engine re-checks every script
    // request against the capability's full validated allowlist anyway.
    ctx.set("origin", origin)
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

    // Bounded host JSON: `package`/`require` are absent, so safe decoding is
    // a host primitive, not a script dependency. Both directions surface
    // static error categories only and respect the response-body bound.
    let json_table = lua
        .create_table()
        .map_err(|_| CollectorError::Internal("context"))?;
    let json_bound = limits.http_response_body_bytes;
    let decode = lua
        .create_function(move |lua, (text,): (String,)| {
            if text.len() > json_bound {
                return Err(mlua::Error::RuntimeError("quota_json_overflow".into()));
            }
            let value: Value = serde_json::from_str(&text)
                .map_err(|_| mlua::Error::RuntimeError("quota_json_malformed".into()))?;
            lua.to_value(&value)
                .map_err(|_| mlua::Error::RuntimeError("quota_json_malformed".into()))
        })
        .map_err(|_| CollectorError::Internal("context"))?;
    let encode = lua
        .create_function(move |lua, value: LuaValue| {
            let value: Value = lua
                .from_value(value)
                .map_err(|_| mlua::Error::RuntimeError("quota_json_malformed".into()))?;
            let text = serde_json::to_string(&value)
                .map_err(|_| mlua::Error::RuntimeError("quota_json_malformed".into()))?;
            if text.len() > json_bound {
                return Err(mlua::Error::RuntimeError("quota_json_overflow".into()));
            }
            Ok(text)
        })
        .map_err(|_| CollectorError::Internal("context"))?;
    json_table
        .set("decode", decode)
        .map_err(|_| CollectorError::Internal("context"))?;
    json_table
        .set("encode", encode)
        .map_err(|_| CollectorError::Internal("context"))?;
    ctx.set("json", json_table)
        .map_err(|_| CollectorError::Internal("context"))?;

    // Bounded host time helper: RFC 3339 reset fields become Unix seconds or
    // nil for unparseable input, preserving unknowns instead of inventing.
    let time_table = lua
        .create_table()
        .map_err(|_| CollectorError::Internal("context"))?;
    let rfc3339 = lua
        .create_function(|_, (text,): (String,)| {
            if text.len() > 64 {
                return Ok(LuaValue::Nil);
            }
            Ok(chrono::DateTime::parse_from_rfc3339(&text)
                .ok()
                .map(|value| value.timestamp_millis() as f64 / 1000.0)
                .map_or(LuaValue::Nil, LuaValue::Number))
        })
        .map_err(|_| CollectorError::Internal("context"))?;
    time_table
        .set("rfc3339", rfc3339)
        .map_err(|_| CollectorError::Internal("context"))?;
    ctx.set("time", time_table)
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
        host_now,
        limits.output_windows,
        MAX_OUTPUT_MODELS,
    )
    .map_err(|error| match error {
        crate::Error::Validation(code) => CollectorError::InvalidOutput(leak_static(code)),
        _ => CollectorError::MalformedOutput,
    })?;
    Ok(snapshot)
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
    } else if let Some(horizon) = text
        .split(HTTP_SENTINEL)
        .nth(1)
        .and_then(|rest| rest.strip_prefix("rate_limited:"))
        .and_then(|digits| {
            digits
                .split(|c: char| c.is_whitespace() || c == ':' || c == '"')
                .next()
        })
        .and_then(|digits| digits.parse::<u64>().ok())
    {
        CollectorError::RateLimited((1..=900).contains(&horizon).then_some(horizon))
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
        "header_forbidden" => Some(QuotaHttpError::HeaderForbidden),
        "rate_limited" => Some(QuotaHttpError::RateLimited),
        _ => None,
    }
}

/// Keeps only the static rejection codes the normalizer itself defines, so no
/// foreign text can ride along in an external error.
fn leak_static(code: String) -> &'static str {
    match code.as_str() {
        "quota_output_malformed" => "quota_output_malformed",
        "quota_output_version" => "quota_output_version",
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
