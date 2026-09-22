//! Embedded Lua collector engine boundary tests with fake HTTP fixtures.

use agent_run_core::capacity::lua::{
    run_collector, AllowedOrigin, AuthCapability, AuthPlacement, CollectorError, CollectorLimits,
    CollectorScript, QuotaHttpClient, QuotaHttpError, QuotaHttpRequest, QuotaHttpResponse,
    ScriptRegistry,
};
use agent_run_core::capacity::quota::CollectorScope;
use agent_run_domain::catalog::{AccountId, NormalizedQuotaSnapshot};
use serde_json::json;
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    path::Path,
    str::FromStr,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tempfile::tempdir;

const SECRET: &str = "synthetic-secret-token-xyz";

/// Recording fixture transport: captures every request that reached it.
struct FakeHttp {
    requests: Mutex<Vec<QuotaHttpRequest>>,
    responses: Mutex<VecDeque<QuotaHttpResponse>>,
}

impl FakeHttp {
    fn new(responses: Vec<QuotaHttpResponse>) -> Arc<Self> {
        Arc::new(Self {
            requests: Mutex::new(Vec::new()),
            responses: Mutex::new(responses.into()),
        })
    }

    fn issued(&self) -> usize {
        self.requests.lock().unwrap().len()
    }
}

impl QuotaHttpClient for FakeHttp {
    fn fetch<'a>(
        &'a self,
        request: QuotaHttpRequest,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = Result<QuotaHttpResponse, QuotaHttpError>> + Send + 'a,
        >,
    > {
        Box::pin(async move {
            self.requests.lock().unwrap().push(request);
            self.responses
                .lock()
                .unwrap()
                .pop_front()
                .ok_or(QuotaHttpError::Transport)
        })
    }
}

fn response(status: u16, headers: &[(&str, &str)], body: &str) -> QuotaHttpResponse {
    QuotaHttpResponse {
        status,
        headers: headers
            .iter()
            .map(|(name, value)| ((*name).to_owned(), (*value).to_owned()))
            .collect(),
        body: body.as_bytes().to_vec(),
    }
}

fn origins() -> Vec<AllowedOrigin> {
    vec![AllowedOrigin::new("https", "api.test", 443).unwrap()]
}

fn account() -> AccountId {
    AccountId::from_str("acct-main").unwrap()
}

fn auth() -> AuthCapability {
    AuthCapability::new(
        account(),
        AuthPlacement::RawAuthorization,
        SECRET.into(),
        origins(),
    )
    .unwrap()
}

fn scope(_script: &CollectorScript) -> CollectorScope {
    CollectorScope {
        runtime: "glm".into(),
        // Stable collector identity: a script revision must not fork pools.
        source: "glm-native".into(),
        models: BTreeSet::from(["glm-4.7".to_owned()]),
    }
}

async fn run(
    source: &str,
    limits: CollectorLimits,
    client: Arc<dyn QuotaHttpClient>,
) -> Result<NormalizedQuotaSnapshot, CollectorError> {
    run_with(source, limits, client, auth()).await
}

async fn run_with(
    source: &str,
    limits: CollectorLimits,
    client: Arc<dyn QuotaHttpClient>,
    auth: AuthCapability,
) -> Result<NormalizedQuotaSnapshot, CollectorError> {
    let script = CollectorScript::new(source);
    let models = BTreeMap::from([("glm-4.7".to_owned(), json!({"tier": "pro"}))]);
    run_collector(
        &script,
        &scope(&script),
        &account(),
        &models,
        1000.0,
        &limits,
        client,
        &auth,
    )
    .await
}

fn fast() -> CollectorLimits {
    CollectorLimits {
        invocation_timeout: Duration::from_secs(5),
        http_request_timeout: Duration::from_secs(2),
        ..CollectorLimits::default()
    }
}

/// A script that fetches quota JSON from the allowed origin and reports it.
const GOOD_SCRIPT: &str = r#"
collect = function(ctx)
  assert(ctx.account.id == "acct-main", "account is host-bound")
  assert(ctx.models["glm-4.7"].tier == "pro", "explicit model config")
  assert(ctx.now == 1000.0, "host time")
  local r = ctx.http.request({url = "https://api.test/quota"})
  local data = ctx.json.decode(r.body)
  assert(ctx.time.rfc3339("1970-01-01T00:16:40Z") == 1000.0, "host time parser")
  return { version = 1, windows = { { pool = "primary", window = "five_hour",
    models = {"glm-4.7"}, remaining_percent = data.remaining_percent,
    reset_at = data.reset_at, observed_at = 1000.0 } } }
end
"#;

#[tokio::test]
async fn valid_collection_binds_account_and_injects_authorization_in_rust() {
    let client = FakeHttp::new(vec![response(
        200,
        &[],
        r#"{"remaining_percent":42.5,"reset_at":2000.0}"#,
    )]);
    let snapshot = run(GOOD_SCRIPT, fast(), client.clone()).await.unwrap();
    snapshot.validate().unwrap();
    assert_eq!(snapshot.account, account());
    assert_eq!(
        snapshot.models[0].pools[0].key.as_str(),
        "acct-main::primary"
    );
    let requests = client.requests.lock().unwrap();
    assert_eq!(requests.len(), 1);
    let (name, value) = requests[0]
        .headers
        .iter()
        .find(|(name, _)| name == "authorization")
        .expect("authorization injected by Rust");
    assert_eq!(name, "authorization");
    assert_eq!(value, SECRET);
}

#[tokio::test]
async fn secret_holders_never_leak_through_debug() {
    // Root repro 1: derived Debug of the capability and of the injected
    // request must never contain credential material.
    let capability = auth();
    assert!(!format!("{capability:?}").contains(SECRET));
    let client = FakeHttp::new(vec![response(200, &[], "{}")]);
    run(GOOD_SCRIPT, fast(), client.clone()).await.unwrap();
    let requests = client.requests.lock().unwrap();
    let debug = format!("{:?}", requests[0]);
    assert!(
        !debug.contains(SECRET),
        "request Debug must be redacted: {debug}"
    );
    assert!(debug.contains("[redacted]"));
}

#[tokio::test]
async fn forbidden_origin_is_rejected_before_any_credential_access() {
    let client = FakeHttp::new(vec![]);
    let err = run(
        r#"collect = function(ctx) ctx.http.request({url = "https://evil.test/quota"}) end"#,
        fast(),
        client.clone(),
    )
    .await
    .unwrap_err();
    assert_eq!(err, CollectorError::Http(QuotaHttpError::OriginForbidden));
    assert_eq!(client.issued(), 0, "credential never reached transport");

    // Userinfo, fragments, and wrong ports are equally forbidden.
    for url in [
        "https://user:pass@api.test/quota",
        "https://api.test/quota#frag",
        "https://api.test:8443/quota",
    ] {
        let client = FakeHttp::new(vec![]);
        let script = format!("collect = function(ctx) ctx.http.request({{url = \"{url}\"}}) end");
        let err = run(&script, fast(), client.clone()).await.unwrap_err();
        assert_eq!(err, CollectorError::Http(QuotaHttpError::OriginForbidden));
        assert_eq!(client.issued(), 0);
    }
}

#[tokio::test]
async fn non_https_origins_are_rejected_at_construction_and_invocation() {
    // Root repro 3: plain HTTP away from loopback can never become an origin.
    assert!(AllowedOrigin::new("http", "api.example.com", 80).is_err());
    let loopback = AllowedOrigin::new("http", "127.0.0.1", 8080).unwrap();
    assert!(loopback.allows("http://127.0.0.1:8080/quota"));
    assert!(!loopback.allows("http://127.0.0.1:8081/quota"));
    // A capability built with an invalid origin cannot be constructed at all,
    // so no invocation can be widened to it through the public API.
    assert!(AuthCapability::new(
        account(),
        AuthPlacement::RawAuthorization,
        SECRET.into(),
        vec![AllowedOrigin {
            scheme: "http".into(),
            host: "api.example.com".into(),
            port: 80,
        }],
    )
    .is_err());
}

#[tokio::test]
async fn capability_is_bound_to_one_account() {
    let other = AuthCapability::new(
        AccountId::from_str("acct-other").unwrap(),
        AuthPlacement::RawAuthorization,
        SECRET.into(),
        origins(),
    )
    .unwrap();
    let err = run_with(
        "collect = function(ctx) end",
        fast(),
        FakeHttp::new(vec![]),
        other,
    )
    .await
    .unwrap_err();
    assert_eq!(err, CollectorError::Internal("auth_account"));
}

#[tokio::test]
async fn cross_origin_redirects_are_rejected_and_same_origin_followed() {
    let client = FakeHttp::new(vec![
        response(302, &[("location", "https://evil.test/x")], ""),
        response(200, &[], r#"{"remaining_percent":10.0}"#),
    ]);
    let err = run(GOOD_SCRIPT, fast(), client.clone()).await.unwrap_err();
    assert_eq!(
        err,
        CollectorError::Http(QuotaHttpError::CrossOriginRedirect)
    );
    assert_eq!(client.issued(), 1, "credentials never followed off-origin");

    let client = FakeHttp::new(vec![
        response(302, &[("location", "https://api.test/other")], ""),
        response(200, &[], r#"{"remaining_percent":11.0,"reset_at":2000.0}"#),
    ]);
    let snapshot = run(GOOD_SCRIPT, fast(), client.clone()).await.unwrap();
    assert_eq!(
        snapshot.models[0].pools[0].windows[0].remaining_percent,
        Some(11.0)
    );
    assert_eq!(client.issued(), 2);
}

#[tokio::test]
async fn redirect_hops_count_against_the_shared_budget() {
    // Three same-origin hops under a budget of two: exactly two attempts run.
    let client = FakeHttp::new(vec![
        response(302, &[("location", "https://api.test/a")], ""),
        response(302, &[("location", "https://api.test/b")], ""),
        response(200, &[], r#"{"remaining_percent":1.0}"#),
    ]);
    let limits = CollectorLimits {
        http_requests: 2,
        ..fast()
    };
    let err = run(GOOD_SCRIPT, limits, client.clone()).await.unwrap_err();
    assert_eq!(err, CollectorError::Http(QuotaHttpError::BudgetExhausted));
    assert_eq!(client.issued(), 2, "every hop consumes the budget");
}

#[tokio::test]
async fn exhausted_budget_stays_exhausted_through_pcall_retries() {
    // Root finding E: pcall catching each failure must not buy more requests
    // than the budget allows; successes are counted from granted requests.
    let ok_responses: Vec<QuotaHttpResponse> = (0..20).map(|_| response(200, &[], "{}")).collect();
    let client = FakeHttp::new(ok_responses);
    let limits = CollectorLimits {
        http_requests: 4,
        ..fast()
    };
    let script = r#"
collect = function(ctx)
  local granted = 0
  for i = 1, 20 do
    local ok = pcall(function() ctx.http.request({url = "https://api.test/q"}) end)
    if ok then granted = granted + 1 end
  end
  return { version = 1, windows = { { pool = "primary", window = "five_hour",
    models = {"glm-4.7"}, remaining_percent = granted, observed_at = 1000.0 } } }
end
"#;
    let snapshot = run(script, limits, client.clone()).await.unwrap();
    assert_eq!(client.issued(), 4, "checked budget never underflows");
    assert_eq!(
        snapshot.models[0].pools[0].windows[0].remaining_percent,
        Some(4.0)
    );
}

#[tokio::test]
async fn script_supplied_auth_and_routing_headers_are_blocked() {
    for header in [
        "authorization",
        "Authorization",
        "host",
        "proxy-authorization",
    ] {
        let client = FakeHttp::new(vec![]);
        let script = format!(
            r#"collect = function(ctx)
  local ok = pcall(function()
    ctx.http.request({{url = "https://api.test/q", headers = {{["{header}"] = "x"}} }})
  end)
  if ok then error("override accepted") end
  return {{ version = 1, windows = {{ {{ pool = "primary", window = "five_hour",
    models = {{"glm-4.7"}}, remaining_percent = 5.0, observed_at = 1000.0 }} }} }}
end
"#
        );
        let snapshot = run(&script, fast(), client.clone()).await.unwrap();
        assert_eq!(snapshot.models.len(), 1, "header {header} blocked");
        assert_eq!(client.issued(), 0);
    }
    // Custom placement headers are equally protected from scripts.
    let client = FakeHttp::new(vec![response(
        200,
        &[],
        r#"{"remaining_percent":9.0,"reset_at":2000.0}"#,
    )]);
    let gateway = AuthCapability::new(
        account(),
        AuthPlacement::Header("x-api-key".into()),
        SECRET.into(),
        origins(),
    )
    .unwrap();
    let script = r#"
collect = function(ctx)
  local ok = pcall(function()
    ctx.http.request({url = "https://api.test/q", headers = {["x-api-key"] = "spoof"} })
  end)
  if ok then error("x-api-key override accepted") end
  local r = ctx.http.request({url = "https://api.test/q"})
  local v = r.body:match('"remaining_percent":([%d%.]+)')
  return { version = 1, windows = { { pool = "primary", window = "five_hour",
    models = {"glm-4.7"}, remaining_percent = tonumber(v), observed_at = 1000.0 } } }
end
"#;
    let snapshot = run_with(script, fast(), client.clone(), gateway)
        .await
        .unwrap();
    assert_eq!(
        snapshot.models[0].pools[0].windows[0].remaining_percent,
        Some(9.0)
    );
    let requests = client.requests.lock().unwrap();
    assert_eq!(
        requests[0].headers[0],
        ("x-api-key".to_owned(), SECRET.to_owned())
    );
}

#[tokio::test]
async fn echoed_credentials_are_scrubbed_before_lua_can_read_them() {
    // Root finding B: a fixture echoing the injected secret in body and
    // headers must not let the script observe it.
    let echo = format!("{{\"echo\":\"{SECRET}\",\"remaining_percent\":42.0}}");
    let client = FakeHttp::new(vec![response(
        200,
        &[
            ("x-echo-auth", SECRET),
            ("content-type", "application/json"),
        ],
        &echo,
    )]);
    let needle = &SECRET["synthetic-secret".len()..];
    let script = format!(
        r#"
collect = function(ctx)
  local r = ctx.http.request({{url = "https://api.test/quota"}})
  local leaked = (r.body:find("{needle}", 1, true) ~= nil)
    or (r.headers["x-echo-auth"] ~= nil and r.headers["x-echo-auth"]:find("{needle}", 1, true) ~= nil)
  local v = r.body:match('"remaining_percent":([%d%.]+)')
  if leaked then v = "55.0" end
  return {{ version = 1, windows = {{ {{ pool = "primary", window = "five_hour",
    models = {{"glm-4.7"}}, remaining_percent = tonumber(v), observed_at = 1000.0 }} }} }}
end
"#
    );
    let snapshot = run(&script, fast(), client).await.unwrap();
    assert_eq!(
        snapshot.models[0].pools[0].windows[0].remaining_percent,
        Some(42.0),
        "secret material must not be observable from Lua"
    );
}

#[tokio::test]
async fn instruction_budget_and_wall_deadline_abort_cpu_loops() {
    let limits = CollectorLimits {
        instructions: 200_000,
        ..fast()
    };
    let err = run(
        r#"collect = function(ctx) local x = 0 while true do x = x + 1 end end"#,
        limits,
        FakeHttp::new(vec![]),
    )
    .await
    .unwrap_err();
    assert_eq!(err, CollectorError::InstructionLimit);

    let limits = CollectorLimits {
        instructions: 100_000_000,
        invocation_timeout: Duration::from_millis(150),
        ..fast()
    };
    let err = run(
        r#"collect = function(ctx) local x = 0 while true do x = x + 1 end end"#,
        limits,
        FakeHttp::new(vec![]),
    )
    .await
    .unwrap_err();
    assert_eq!(err, CollectorError::Timeout);
}

#[tokio::test]
async fn catchable_hook_errors_cannot_outrun_the_invocation_deadline() {
    // Root finding G, executed repro: hook errors are ordinary catchable Lua
    // errors and the outer async timeout cannot preempt a poll that never
    // yields. The guarded pcall/xpcall escalation must terminate this exact
    // script on its own, with a typed bound failure and no external kill.
    let limits = CollectorLimits {
        invocation_timeout: Duration::from_millis(100),
        ..fast()
    };
    let exact = "while true do pcall(function() while true do end end) end";
    for script in [
        exact,
        // The same escape through xpcall and a coroutine resumer.
        "local co = coroutine.wrap(function() local x = 0 while true do x = x + 1 end end) \
         while true do xpcall(co, function() end) pcall(function() local y = 0 while true do y = y + 1 end end) end",
        // An instruction-budget catcher collapses the same way.
        "while true do pcall(function() local x = 0 while true do x = x + 1 end end) end",
    ] {
        let limits = if script == exact {
            limits
        } else {
            CollectorLimits {
                instructions: 10_000,
                ..limits
            }
        };
        let started = Instant::now();
        let err = run(script, limits, FakeHttp::new(vec![])).await.unwrap_err();
        assert!(
            matches!(err, CollectorError::Timeout | CollectorError::InstructionLimit),
            "unexpected {err:?}"
        );
        assert!(
            started.elapsed() < Duration::from_secs(3),
            "terminated on its own without a watchdog"
        );
    }
}

#[tokio::test]
async fn memory_budget_aborts_allocation_heavy_scripts() {
    let limits = CollectorLimits {
        vm_memory_bytes: 256 * 1024,
        ..fast()
    };
    let err = run(
        r#"collect = function(ctx) local t = {} for i = 1, 100000 do t[i] = string.rep("x", 4096) end end"#,
        limits,
        FakeHttp::new(vec![]),
    )
    .await
    .unwrap_err();
    assert_eq!(err, CollectorError::MemoryLimit);
}

#[tokio::test]
async fn unsafe_libraries_and_loaders_are_absent() {
    let client = FakeHttp::new(vec![]);
    let snapshot = run(
        r#"
collect = function(ctx)
  assert(io == nil and os == nil and debug == nil and package == nil, "unsafe library present")
  assert(load == nil and loadstring == nil and dofile == nil and require == nil, "loader present")
  return { version = 1, windows = { { pool = "primary", window = "five_hour",
    models = {"glm-4.7"}, remaining_percent = 5.0, observed_at = 1000.0 } } }
end
"#,
        fast(),
        client,
    )
    .await
    .unwrap();
    assert_eq!(snapshot.models.len(), 1);

    // Touching an absent library is a script failure with its text discarded.
    let err = run(
        r#"collect = function(ctx) io.read() end"#,
        fast(),
        FakeHttp::new(vec![]),
    )
    .await
    .unwrap_err();
    assert_eq!(err, CollectorError::ScriptFailed);
}

#[tokio::test]
async fn bytecode_and_tampered_scripts_are_rejected() {
    let err = run("\u{1b}Lua\x54garbage", fast(), FakeHttp::new(vec![]))
        .await
        .unwrap_err();
    assert_eq!(err, CollectorError::BytecodeRejected);

    let mut tampered = CollectorScript::new("collect = function(ctx) end");
    tampered.sha256 = "0".repeat(64);
    let models = BTreeMap::new();
    let err = run_collector(
        &tampered,
        &scope(&tampered),
        &account(),
        &models,
        1000.0,
        &fast(),
        FakeHttp::new(vec![]),
        &auth(),
    )
    .await
    .unwrap_err();
    assert_eq!(err, CollectorError::ScriptTampered);
}

#[tokio::test]
async fn registry_rejects_non_compiling_replacement_and_keeps_valid_revision() {
    // Root repro 2: a syntax-invalid script must never displace a retained
    // valid revision, and installation compiles in text mode first.
    let mut registry = ScriptRegistry::default();
    let valid = CollectorScript::new("collect = function(ctx) end");
    registry.install("glm", valid.clone(), &fast()).unwrap();
    assert!(registry
        .install(
            "glm",
            CollectorScript::new("this is not valid Lua !!!!!"),
            &fast()
        )
        .is_err());
    assert_eq!(registry.get("glm"), Some(&valid));
}

#[tokio::test]
async fn request_budget_response_bound_and_output_bound_are_enforced() {
    // Response body bound.
    let big = "x".repeat(3 * 1024 * 1024);
    let client = FakeHttp::new(vec![response(200, &[], &big)]);
    let err = run(GOOD_SCRIPT, fast(), client).await.unwrap_err();
    assert_eq!(err, CollectorError::Http(QuotaHttpError::BodyTooLarge));

    // Output window bound.
    let script = "collect = function(ctx) return { version = 1, windows = { \
        { pool = 'primary', window = 'w0', models = {'glm-4.7'}, observed_at = 1000.0 }, \
        { pool = 'primary', window = 'w1', models = {'glm-4.7'}, observed_at = 1000.0 }, \
        { pool = 'primary', window = 'w2', models = {'glm-4.7'}, observed_at = 1000.0 } } } end";
    let limits = CollectorLimits {
        output_windows: 2,
        ..fast()
    };
    let err = run(script, limits, FakeHttp::new(vec![]))
        .await
        .unwrap_err();
    assert_eq!(err, CollectorError::InvalidOutput("quota_output_overflow"));

    // Limits themselves are validated before anything runs.
    let err = run(
        "collect = function(ctx) end",
        CollectorLimits {
            instructions: 0,
            ..fast()
        },
        FakeHttp::new(vec![]),
    )
    .await
    .unwrap_err();
    assert_eq!(err, CollectorError::Internal("limits"));
}

#[tokio::test]
async fn malformed_and_foreign_outputs_fail_with_stable_categories() {
    let err = run(
        r#"collect = function(ctx) return {} end"#,
        fast(),
        FakeHttp::new(vec![]),
    )
    .await
    .unwrap_err();
    assert_eq!(
        err,
        CollectorError::InvalidOutput("quota_output_version"),
        "version envelope is mandatory"
    );

    let err = run(
        r#"collect = function(ctx) return { windows = {} } end"#,
        fast(),
        FakeHttp::new(vec![]),
    )
    .await
    .unwrap_err();
    assert_eq!(err, CollectorError::InvalidOutput("quota_output_version"));

    let err = run(
        r#"collect = function(ctx) return 42 end"#,
        fast(),
        FakeHttp::new(vec![]),
    )
    .await
    .unwrap_err();
    assert_eq!(err, CollectorError::InvalidOutput("quota_output_malformed"));

    let err = run(
        r#"collect = function(ctx)
              return { version = 1, windows = { { pool = "primary", window = "five_hour",
                                     models = {"gpt-5.1"}, remaining_percent = 1.0,
                                     observed_at = 1000.0 } } }
           end"#,
        fast(),
        FakeHttp::new(vec![]),
    )
    .await
    .unwrap_err();
    assert_eq!(
        err,
        CollectorError::InvalidOutput("quota_output_foreign_model")
    );

    let err = run(
        r#"collect = function(ctx)
              return { version = 1, windows = { { pool = "primary", window = "five_hour",
                                     models = {"glm-4.7"}, remaining_percent = 101.0,
                                     observed_at = 1000.0 } } }
           end"#,
        fast(),
        FakeHttp::new(vec![]),
    )
    .await
    .unwrap_err();
    assert_eq!(
        err,
        CollectorError::InvalidOutput("quota_output_invalid_number")
    );

    let err = run(
        r#"collect = function(ctx)
              return { version = 1, windows = { { pool = "primary", window = "five_hour",
                                     models = {"glm-4.7"}, remaining_percent = 1.0,
                                     observed_at = 100000.0 } } }
           end"#,
        fast(),
        FakeHttp::new(vec![]),
    )
    .await
    .unwrap_err();
    assert_eq!(
        err,
        CollectorError::InvalidOutput("quota_output_invalid_time")
    );
}

#[tokio::test]
async fn secrets_never_surface_in_errors_or_diagnostics() {
    // A fixture endpoint echoes the injected credential back; the script then
    // fails with that body as its error text.
    let echo = format!("{{\"authorization\":\"{SECRET}\",\"remaining_percent\":1.0}}");
    let client = FakeHttp::new(vec![response(200, &[], &echo)]);
    let err = run(
        r#"collect = function(ctx) local r = ctx.http.request({url = "https://api.test/quota"}) error(r.body) end"#,
        fast(),
        client,
    )
    .await
    .unwrap_err();
    assert_eq!(err, CollectorError::ScriptFailed);
    assert!(!format!("{err}").contains(SECRET));
    assert!(!format!("{err:?}").contains(SECRET));

    // The redaction helper scrubs every known marker, value first.
    let auth = auth();
    assert_eq!(
        auth.redact(&format!("head {SECRET} tail")),
        "head [redacted] tail"
    );
}

#[tokio::test]
async fn host_json_and_time_primitives_are_bounded_and_static() {
    // The JSON host primitive decodes real bodies and surfaces only static
    // categories; encode is bounded by the same body bound.
    let client = FakeHttp::new(vec![response(
        200,
        &[],
        r#"{"remaining_percent":42.5,"reset_at":2000.0}"#,
    )]);
    let snapshot = run(GOOD_SCRIPT, fast(), client).await.unwrap();
    let window = &snapshot.models[0].pools[0].windows[0];
    assert_eq!(window.remaining_percent, Some(42.5));
    assert_eq!(window.reset_at, Some(2000.0));

    let script = r#"
collect = function(ctx)
  local ok1 = pcall(function() ctx.json.decode("{not json") end)
  local ok2, text = pcall(function() return ctx.json.encode({a = 1}) end)
  assert(ok2 and text == '{"a":1}', "encode roundtrip")
  assert(ctx.time.rfc3339("garbage") == nil, "unparseable stays unknown")
  if ok1 then error("malformed JSON accepted") end
  return { version = 1, windows = { { pool = "primary", window = "five_hour",
    models = {"glm-4.7"}, remaining_percent = 1.0, observed_at = 1000.0 } } }
end
"#;
    let snapshot = run(script, fast(), FakeHttp::new(vec![])).await.unwrap();
    assert_eq!(snapshot.models.len(), 1);

    // Oversized decode input is rejected by the static bound.
    let big = format!("{{\"pad\":\"{}\"}}", "x".repeat(4096));
    let script = format!(
        r#"collect = function(ctx)
  local ok = pcall(function() ctx.json.decode({}) end)
  if ok then error("oversized JSON accepted") end
  return {{ version = 1, windows = {{ {{ pool = "primary", window = "five_hour",
    models = {{"glm-4.7"}}, remaining_percent = 1.0, observed_at = 1000.0 }} }} }}
end
"#,
        string_literal(&big)
    );
    let limits = CollectorLimits {
        http_response_body_bytes: 1024,
        ..fast()
    };
    let snapshot = run(&script, limits, FakeHttp::new(vec![])).await.unwrap();
    assert_eq!(snapshot.models.len(), 1);
}

/// Encodes `text` as a quoted Lua string literal with escapes.
fn string_literal(text: &str) -> String {
    let mut out = String::from("\"");
    for ch in text.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            _ => out.push(ch),
        }
    }
    out.push('"');
    out
}

#[tokio::test]
async fn collector_output_persists_through_the_quota_store_path() {
    // End-to-end A+B: a valid Lua round normalizes, then persists with the
    // latch and one revision advance in the same transaction.
    let home = tempdir().unwrap();
    agent_run_store::Store::initialize(home.path()).unwrap();
    let script = CollectorScript::new(
        r#"
collect = function(ctx)
  return { version = 1, windows = { { pool = "primary", window = "five_hour", models = {"glm-4.7"},
                        remaining_percent = 0.0, reset_at = 2000.0, observed_at = ctx.now } } }
end
"#,
    );
    let snapshot = run_collector(
        &script,
        &scope(&script),
        &account(),
        &BTreeMap::from([("glm-4.7".to_owned(), json!({}))]),
        1000.0,
        &fast(),
        FakeHttp::new(vec![]),
        &auth(),
    )
    .await
    .unwrap();
    let revision = agent_run_store::quota::record_quota_snapshot(
        Path::new(home.path()),
        "glm",
        &snapshot,
        100,
        1500.0,
    )
    .unwrap();
    assert_eq!(revision, 1);
}
