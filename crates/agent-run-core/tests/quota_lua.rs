//! Embedded Lua collector engine boundary tests with fake HTTP fixtures.

use agent_run_core::capacity::lua::{
    run_collector, AllowedOrigin, AuthCapability, CollectorError, CollectorLimits, CollectorScript,
    QuotaHttpClient, QuotaHttpError, QuotaHttpRequest, QuotaHttpResponse, ScriptRegistry,
};
use agent_run_core::capacity::quota::CollectorScope;
use agent_run_domain::catalog::{AccountId, NormalizedQuotaSnapshot};
use serde_json::json;
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    path::Path,
    str::FromStr,
    sync::{Arc, Mutex},
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
    vec![AllowedOrigin {
        scheme: "https".into(),
        host: "api.test".into(),
        port: 443,
    }]
}

async fn run(
    source: &str,
    limits: CollectorLimits,
    client: Arc<dyn QuotaHttpClient>,
) -> Result<NormalizedQuotaSnapshot, CollectorError> {
    let script = CollectorScript::new(source);
    let scope = CollectorScope {
        runtime: "glm".into(),
        source: format!("lua:{}", &script.sha256[..8]),
        models: BTreeSet::from(["glm-4.7".to_owned()]),
    };
    let models = BTreeMap::from([("glm-4.7".to_owned(), json!({"tier": "pro"}))]);
    let auth = AuthCapability::new(SECRET.into(), vec![SECRET.to_owned()]);
    run_collector(
        &script,
        &scope,
        &AccountId::from_str("acct-main").unwrap(),
        &models,
        1000.0,
        &limits,
        &origins(),
        client,
        &auth,
    )
    .await
}

fn fast() -> CollectorLimits {
    CollectorLimits {
        invocation_timeout: std::time::Duration::from_secs(5),
        http_request_timeout: std::time::Duration::from_secs(2),
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
  local data = require_nothing(r)
  return { windows = data }
end
function require_nothing(r)
  local decoded = json_decode(r.body)
  return decoded
end
function json_decode(body)
  local limits = body:match('"remaining_percent":([%d%.]+)')
  return { { pool = "primary", window = "five_hour", models = {"glm-4.7"},
            remaining_percent = tonumber(limits), observed_at = 1000.0 } }
end
"#;

#[tokio::test]
async fn valid_collection_binds_account_and_injects_authorization_in_rust() {
    let client = FakeHttp::new(vec![response(
        200,
        &[],
        r#"{"remaining_percent":42.5,"limits":{}}"#,
    )]);
    let snapshot = run(GOOD_SCRIPT, fast(), client.clone()).await.unwrap();
    snapshot.validate().unwrap();
    assert_eq!(snapshot.account, AccountId::from_str("acct-main").unwrap());
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
        "http://api.test/quota",
    ] {
        let client = FakeHttp::new(vec![]);
        let script = format!("collect = function(ctx) ctx.http.request({{url = \"{url}\"}}) end");
        let err = run(&script, fast(), client.clone()).await.unwrap_err();
        assert_eq!(err, CollectorError::Http(QuotaHttpError::OriginForbidden));
        assert_eq!(client.issued(), 0);
    }
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
        response(200, &[], r#"{"remaining_percent":11.0}"#),
    ]);
    let snapshot = run(GOOD_SCRIPT, fast(), client.clone()).await.unwrap();
    assert_eq!(
        snapshot.models[0].pools[0].windows[0].remaining_percent,
        Some(11.0)
    );
    assert_eq!(client.issued(), 2);
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
        invocation_timeout: std::time::Duration::from_millis(150),
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
  return { windows = { { pool = "primary", window = "five_hour", models = {"glm-4.7"},
                        remaining_percent = 5.0, observed_at = 1000.0 } } }
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
    let scope = CollectorScope {
        runtime: "glm".into(),
        source: "lua:t".into(),
        models: BTreeSet::from(["glm-4.7".to_owned()]),
    };
    let err = run_collector(
        &tampered,
        &scope,
        &AccountId::from_str("acct-main").unwrap(),
        &BTreeMap::new(),
        1000.0,
        &fast(),
        &origins(),
        FakeHttp::new(vec![]),
        &AuthCapability::new(SECRET.into(), vec![SECRET.to_owned()]),
    )
    .await
    .unwrap_err();
    assert_eq!(err, CollectorError::ScriptTampered);

    // A retained registry revision survives a rejected replacement.
    let mut registry = ScriptRegistry::default();
    let valid = CollectorScript::new("collect = function(ctx) end");
    registry.install("glm", valid.clone()).unwrap();
    let mut bad = CollectorScript::new("collect = function(ctx) end");
    bad.sha256 = "1".repeat(64);
    assert!(registry.install("glm", bad).is_err());
    assert_eq!(registry.get("glm"), Some(&valid));
}

#[tokio::test]
async fn request_budget_response_bound_and_output_bound_are_enforced() {
    // Shared request budget: 8 requests, script asks for 9.
    let responses: Vec<_> = (0..9)
        .map(|_| response(200, &[], r#"{"remaining_percent":1.0}"#))
        .collect();
    let client = FakeHttp::new(responses);
    let err = run(
        r#"collect = function(ctx) for i = 1, 9 do ctx.http.request({url = "https://api.test/q"}) end end"#,
        fast(),
        client.clone(),
    )
    .await
    .unwrap_err();
    assert_eq!(err, CollectorError::Http(QuotaHttpError::BudgetExhausted));
    assert_eq!(client.issued(), 8);

    // Response body bound.
    let big = "x".repeat(3 * 1024 * 1024);
    let client = FakeHttp::new(vec![response(200, &[], &big)]);
    let err = run(GOOD_SCRIPT, fast(), client).await.unwrap_err();
    assert_eq!(err, CollectorError::Http(QuotaHttpError::BodyTooLarge));

    // Output window bound.
    let script = "collect = function(ctx) return { windows = { \
        { pool = 'primary', window = 'w0', models = {'glm-4.7'}, observed_at = 1000.0 }, \
        { pool = 'primary', window = 'w1', models = {'glm-4.7'}, observed_at = 1000.0 }, \
        { pool = 'primary', window = 'w2', models = {'glm-4.7'}, observed_at = 1000.0 } } } end";
    let limits = CollectorLimits {
        output_windows: 2,
        ..fast()
    };
    let err = run(&script, limits, FakeHttp::new(vec![]))
        .await
        .unwrap_err();
    assert_eq!(err, CollectorError::InvalidOutput("quota_output_overflow"));
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
    assert_eq!(err, CollectorError::InvalidOutput("quota_output_malformed"));

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
              return { windows = { { pool = "primary", window = "five_hour",
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
              return { windows = { { pool = "primary", window = "five_hour",
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

    // The redaction helper scrubs every known marker.
    let auth = AuthCapability::new(SECRET.into(), vec![SECRET.to_owned()]);
    assert_eq!(
        auth.redact(&format!("head {SECRET} tail")),
        "head [redacted] tail"
    );
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
  return { windows = { { pool = "primary", window = "five_hour", models = {"glm-4.7"},
                        remaining_percent = 0.0, reset_at = 2000.0, observed_at = ctx.now } } }
end
"#,
    );
    let scope = CollectorScope {
        runtime: "glm".into(),
        source: format!("lua:{}", &script.sha256[..8]),
        models: BTreeSet::from(["glm-4.7".to_owned()]),
    };
    let auth = AuthCapability::new(SECRET.into(), vec![SECRET.to_owned()]);
    let snapshot = run_collector(
        &script,
        &scope,
        &AccountId::from_str("acct-main").unwrap(),
        &BTreeMap::from([("glm-4.7".to_owned(), json!({}))]),
        1000.0,
        &fast(),
        &origins(),
        FakeHttp::new(vec![]),
        &auth,
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
