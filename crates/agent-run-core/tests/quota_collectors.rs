//! Account-scoped first-party collector driver: dedup, auth isolation,
//! backoff, and real response normalization through fixture transports.

use agent_run_adapters::authorized_request::{CredentialReader, SystemCredentialReader};
use agent_run_core::capacity::collectors::{
    collect_provider_quota, first_party, planned_pairs, AccountBackoff,
};
use agent_run_core::capacity::lua::{
    CollectorLimits, QuotaHttpClient, QuotaHttpError, QuotaHttpRequest, QuotaHttpResponse,
};
use agent_run_domain::catalog::{
    AccountId, AccountRecord, AccountStatus, AuthFamily, CollectorBinding, HarnessId, LimitsSource,
    ProviderBinding, ProviderCatalog, ProviderConnection, ProviderDefinition, ProviderId,
    ProviderModel, ProviderProtocol,
};
use agent_run_domain::types::PositiveFinite;
use agent_run_domain::CredentialRef;
use rusqlite::Connection;
use std::{
    collections::VecDeque,
    path::Path,
    str::FromStr,
    sync::{Arc, Mutex},
    time::Duration,
};
use tempfile::tempdir;

/// The validated default ranking multiplier.
fn one() -> PositiveFinite {
    PositiveFinite::try_from(1.0).unwrap()
}

const GLM_SECRET: &str = "glm-protected-token-a";
const CLAUDE_SECRET: &str = "claude-protected-token-b";

/// Fixed transport that answers every allowed request with queued responses.
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

fn response(status: u16, body: &str) -> QuotaHttpResponse {
    QuotaHttpResponse {
        status,
        headers: vec![],
        body: body.as_bytes().to_vec(),
    }
}

/// Resolves env-style protected references from a fixed fake store.
struct FakeReader;

impl CredentialReader for FakeReader {
    fn read(&self, reference: &CredentialRef) -> agent_run_domain::Result<String> {
        match reference {
            CredentialRef::Environment(name) if name == "GLM_QUOTA_TOKEN" => {
                Ok(GLM_SECRET.to_owned())
            }
            CredentialRef::Environment(name) if name == "CLAUDE_QUOTA_TOKEN" => {
                Ok(CLAUDE_SECRET.to_owned())
            }
            _ => SystemCredentialReader.read(reference),
        }
    }
}

fn account(id: &str) -> AccountId {
    AccountId::from_str(id).unwrap()
}

/// One provider definition with a Lua collector bound to `script`.
fn lua_provider(
    id: &str,
    script: &str,
    origin: &str,
    model: (&str, Option<&str>),
    bound: &AccountId,
) -> ProviderDefinition {
    ProviderDefinition {
        id: ProviderId::from_str(id).unwrap(),
        harness: HarnessId::ClaudeCode,
        connection: ProviderConnection::Custom {
            endpoint: origin.to_owned(),
            protocol: ProviderProtocol::Messages,
            auth_header: Default::default(),
            allow_loopback_http: false,
        },
        auth_family: AuthFamily::from_str("anthropic").unwrap(),
        recommendations: vec![],
        priority_multiplier: one(),
        limits_source: LimitsSource::Lua,
        collector: Some(CollectorBinding {
            script: script.to_owned(),
            origins: vec![format!("{origin}/")],
            script_file: None,
            auth: None,
        }),
        models: vec![ProviderModel {
            id: model.0.to_owned(),
            native_model: model.1.map(str::to_owned),
            params: Default::default(),
            allowed_params: Default::default(),
            recommendations: vec![],
            restrictions: vec![],
        }],
        bindings: vec![ProviderBinding {
            label: "main".parse().unwrap(),
            account: bound.clone(),
            models: None,
            multiplier: one(),
        }],
    }
}

/// Builds a catalog whose accounts reference fake protected env stores.
fn catalog(providers: Vec<ProviderDefinition>, accounts: &[(&str, &str, &str)]) -> ProviderCatalog {
    let records = accounts
        .iter()
        .map(|(id, family, reference)| AccountRecord {
            account_id: account(id),
            auth_family: AuthFamily::from_str(family).unwrap(),
            secret_ref: reference.parse().unwrap(),
            status: AccountStatus::Enabled,
        })
        .collect();
    ProviderCatalog::new(records, providers).unwrap()
}

fn limits() -> CollectorLimits {
    CollectorLimits {
        invocation_timeout: Duration::from_secs(5),
        http_request_timeout: Duration::from_secs(2),
        ..CollectorLimits::default()
    }
}

/// The host clock, for horizon assertions.
fn now_epoch() -> f64 {
    agent_run_core::domain::now()
}

/// Official-shape GLM quota payload: data root, limits, TOKENS_LIMIT/TIME_LIMIT.
fn glm_payload(used: f64) -> String {
    format!(
        r#"{{"code":200,"data":{{"limits":[{{"type":"TOKENS_LIMIT","percentage":{used}}},{{"type":"TIME_LIMIT","percentage":3.0,"currentValue":12}}]}}}}"#
    )
}

/// The recorded Anthropic OAuth usage envelope (unverified live contract).
fn anthropic_payload() -> String {
    r#"{"limits":[
        {"kind":"session","percent":42.0,"resets_at":"1970-01-01T00:50:00Z"},
        {"kind":"weekly_all","percent":10.0}
    ]}"#
    .to_owned()
}

/// Registers `accounts` in the isolated temporary store.
fn registered(home: &Path, accounts: &[(&str, &str, &str)]) {
    let mut store = agent_run_store::Store::initialize(home).unwrap();
    for (id, family, reference) in accounts {
        store
            .register_account(&AccountRecord {
                account_id: account(id),
                auth_family: AuthFamily::from_str(family).unwrap(),
                secret_ref: reference.parse().unwrap(),
                status: AccountStatus::Enabled,
            })
            .unwrap();
    }
}

/// Returns (lane, window, remaining, source) rows persisted for `account`.
fn stored(home: &Path, account: &str) -> Vec<(String, String, Option<f64>, String)> {
    let conn = Connection::open(home.join("state.db")).unwrap();
    let mut stmt = conn
        .prepare(
            "SELECT lane,window,remaining_percent,source FROM capacity_samples \
             WHERE account_id=? ORDER BY lane,window",
        )
        .unwrap();
    stmt.query_map([account], |row| {
        Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
    })
    .unwrap()
    .collect::<rusqlite::Result<Vec<_>>>()
    .unwrap()
}

/// One GLM provider collects its bound account through the official contract.
#[tokio::test]
async fn glm_collector_normalizes_official_payload_and_uses_raw_authorization() {
    let home = tempdir().unwrap();
    let accounts = [("acct-glm", "anthropic", "env:GLM_QUOTA_TOKEN")];
    registered(home.path(), &accounts);
    let provider = lua_provider(
        "glm-main",
        "glm_quota",
        "https://open.bigmodel.cn",
        ("glm-5.3", Some("glm-5.3[1m]")),
        &account("acct-glm"),
    );
    let catalog = catalog(vec![provider], &accounts);
    let client = FakeHttp::new(vec![response(200, &glm_payload(25.0))]);
    let mut backoff = AccountBackoff::default();
    let report = collect_provider_quota(
        home.path(),
        &catalog,
        64,
        &limits(),
        client.clone(),
        &FakeReader,
        &mut backoff,
    )
    .await
    .unwrap();
    assert!(report["ok"].as_bool().unwrap());
    assert_eq!(report["results"][0]["status"], "collected");
    // Exactly one remote request for one physical account.
    assert_eq!(client.issued(), 1);
    {
        let requests = client.requests.lock().unwrap();
        let request = &requests[0];
        assert_eq!(
            request.url,
            "https://open.bigmodel.cn/api/monitor/usage/quota/limit"
        );
        let authorization = request
            .headers
            .iter()
            .find(|(name, _)| name == "authorization")
            .expect("credential injected by Rust");
        // The quota endpoint takes the raw token, not the inference Bearer form.
        assert_eq!(authorization.1, GLM_SECRET);
    }
    // Native model spelling governs membership; TOKENS_LIMIT maps to the
    // five-hour window and TIME_LIMIT is deliberately absent.
    let rows = stored(home.path(), "acct-glm");
    assert_eq!(
        rows,
        vec![(
            "primary".to_owned(),
            "five_hour".to_owned(),
            Some(75.0),
            "glm-quota".to_owned()
        )]
    );
    let conn = Connection::open(home.path().join("state.db")).unwrap();
    let membership: String = conn
        .query_row(
            "SELECT payload_json FROM capacity_samples WHERE account_id='acct-glm'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert!(membership.contains("glm-5.3[1m]"));
}

/// The Anthropic collector ports the recorded envelope with Bearer placement.
#[tokio::test]
async fn anthropic_collector_ports_recorded_envelope_with_bearer_auth() {
    let home = tempdir().unwrap();
    let accounts = [("acct-claude", "anthropic", "env:CLAUDE_QUOTA_TOKEN")];
    registered(home.path(), &accounts);
    let provider = lua_provider(
        "claude-main",
        "anthropic_usage",
        "https://api.anthropic.com",
        ("claude-sonnet-5", None),
        &account("acct-claude"),
    );
    let catalog = catalog(vec![provider], &accounts);
    let client = FakeHttp::new(vec![response(200, &anthropic_payload())]);
    let mut backoff = AccountBackoff::default();
    let report = collect_provider_quota(
        home.path(),
        &catalog,
        64,
        &limits(),
        client.clone(),
        &FakeReader,
        &mut backoff,
    )
    .await
    .unwrap();
    assert!(report["ok"].as_bool().unwrap());
    {
        let requests = client.requests.lock().unwrap();
        let request = &requests[0];
        assert_eq!(request.url, "https://api.anthropic.com/api/oauth/usage");
        let authorization = request
            .headers
            .iter()
            .find(|(name, _)| name == "authorization")
            .unwrap();
        assert_eq!(authorization.1, format!("Bearer {CLAUDE_SECRET}"));
        assert!(request
            .headers
            .iter()
            .any(|(name, value)| { name == "anthropic-beta" && value == "oauth-2025-04-20" }));
    }
    let rows = stored(home.path(), "acct-claude");
    assert_eq!(
        rows,
        vec![
            (
                "primary".to_owned(),
                "five_hour".to_owned(),
                Some(58.0),
                "anthropic-usage".to_owned()
            ),
            (
                "secondary".to_owned(),
                "seven_day".to_owned(),
                Some(90.0),
                "anthropic-usage".to_owned()
            ),
        ]
    );
}

/// Aliases of one global account across two providers collect exactly once.
#[tokio::test]
async fn account_aliases_share_one_remote_request_and_pool() {
    let home = tempdir().unwrap();
    let accounts = [("acct-glm", "anthropic", "env:GLM_QUOTA_TOKEN")];
    registered(home.path(), &accounts);
    let bound = account("acct-glm");
    let catalog = catalog(
        vec![
            lua_provider(
                "glm-a",
                "glm_quota",
                "https://open.bigmodel.cn",
                ("glm-a", Some("glm-5.3[1m]")),
                &bound,
            ),
            lua_provider(
                "glm-b",
                "glm_quota",
                "https://open.bigmodel.cn",
                ("glm-b", None),
                &bound,
            ),
        ],
        &accounts,
    );
    assert_eq!(
        planned_pairs(&catalog).unwrap(),
        [("acct-glm".to_owned(), "glm_quota".to_owned())].into()
    );
    let client = FakeHttp::new(vec![response(200, &glm_payload(10.0))]);
    let mut backoff = AccountBackoff::default();
    let report = collect_provider_quota(
        home.path(),
        &catalog,
        64,
        &limits(),
        client.clone(),
        &FakeReader,
        &mut backoff,
    )
    .await
    .unwrap();
    assert!(report["ok"].as_bool().unwrap());
    assert_eq!(report["results"].as_array().unwrap().len(), 1);
    assert_eq!(client.issued(), 1);
    // Both providers' native spellings share the one physical pool row.
    let conn = Connection::open(home.path().join("state.db")).unwrap();
    let rows: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM capacity_samples WHERE account_id='acct-glm'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(rows, 1);
    let membership: String = conn
        .query_row(
            "SELECT payload_json FROM capacity_samples WHERE account_id='acct-glm'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert!(membership.contains("glm-5.3[1m]") && membership.contains("glm-b"));
}

/// A failed round suppresses the next one for every alias, boundedly, and a
/// success clears it; suppression never issues a remote request.
#[tokio::test]
async fn backoff_after_failure_is_bounded_and_shared_across_aliases() {
    let home = tempdir().unwrap();
    let accounts = [("acct-glm", "anthropic", "env:GLM_QUOTA_TOKEN")];
    registered(home.path(), &accounts);
    let bound = account("acct-glm");
    let catalog = catalog(
        vec![
            lua_provider(
                "glm-a",
                "glm_quota",
                "https://open.bigmodel.cn",
                ("glm-a", None),
                &bound,
            ),
            lua_provider(
                "glm-b",
                "glm_quota",
                "https://open.bigmodel.cn",
                ("glm-b", None),
                &bound,
            ),
        ],
        &accounts,
    );
    let client = FakeHttp::new(vec![]);
    let mut backoff = AccountBackoff::default();
    let failed = collect_provider_quota(
        home.path(),
        &catalog,
        64,
        &limits(),
        client.clone(),
        &FakeReader,
        &mut backoff,
    )
    .await
    .unwrap();
    assert!(!failed["ok"].as_bool().unwrap());
    assert_eq!(
        failed["results"][0]["issues"][0],
        "quota_collector_http_transport"
    );
    assert_eq!(client.issued(), 1);
    let suppressed = collect_provider_quota(
        home.path(),
        &catalog,
        64,
        &limits(),
        client.clone(),
        &FakeReader,
        &mut backoff,
    )
    .await
    .unwrap();
    assert_eq!(suppressed["results"][0]["issues"][0], "backoff");
    assert_eq!(
        client.issued(),
        1,
        "suppressed round issues no remote request for any alias"
    );
    // After the bounded window passes, collection resumes and succeeds.
    backoff.record_success(&bound, "glm-quota");
    let client2 = FakeHttp::new(vec![response(200, &glm_payload(5.0))]);
    let ok = collect_provider_quota(
        home.path(),
        &catalog,
        64,
        &limits(),
        client2.clone(),
        &FakeReader,
        &mut backoff,
    )
    .await
    .unwrap();
    assert!(ok["ok"].as_bool().unwrap());
    assert_eq!(client2.issued(), 1);
}

/// Each account resolves only its own protected reference, and a native
/// harness login is refused instead of becoming an exportable quota token.
#[tokio::test]
async fn credential_resolution_is_per_account_and_refuses_native_logins() {
    let home = tempdir().unwrap();
    let accounts = [
        ("acct-glm", "anthropic", "env:GLM_QUOTA_TOKEN"),
        ("acct-claude", "anthropic", "env:CLAUDE_QUOTA_TOKEN"),
        ("acct-native", "anthropic", "native:claude-code"),
    ];
    registered(home.path(), &accounts);
    let catalog = catalog(
        vec![
            lua_provider(
                "glm-main",
                "glm_quota",
                "https://open.bigmodel.cn",
                ("glm-5.3", None),
                &account("acct-glm"),
            ),
            lua_provider(
                "claude-main",
                "anthropic_usage",
                "https://api.anthropic.com",
                ("claude-sonnet-5", None),
                &account("acct-claude"),
            ),
            lua_provider(
                "native-main",
                "anthropic_usage",
                "https://api.anthropic.com",
                ("claude-opus-5-5", None),
                &account("acct-native"),
            ),
        ],
        &accounts,
    );
    let client = FakeHttp::new(vec![
        response(200, &glm_payload(20.0)),
        response(200, &anthropic_payload()),
    ]);
    let mut backoff = AccountBackoff::default();
    let report = collect_provider_quota(
        home.path(),
        &catalog,
        64,
        &limits(),
        client.clone(),
        &FakeReader,
        &mut backoff,
    )
    .await
    .unwrap();
    let results = report["results"].as_array().unwrap();
    assert_eq!(results.len(), 3);
    {
        let requests = client.requests.lock().unwrap();
        let find = |url: &str| {
            requests
                .iter()
                .find(|request| request.url == url)
                .map(|request| {
                    request
                        .headers
                        .iter()
                        .find(|(name, _)| name == "authorization")
                        .map(|(_, value)| value.clone())
                        .unwrap()
                })
                .expect("one request per resolved account")
        };
        assert_eq!(
            find("https://open.bigmodel.cn/api/monitor/usage/quota/limit"),
            GLM_SECRET
        );
        assert_eq!(
            find("https://api.anthropic.com/api/oauth/usage"),
            format!("Bearer {CLAUDE_SECRET}")
        );
        // The native-login account never reached the network.
        assert_eq!(requests.len(), 2);
    }
    let native = results
        .iter()
        .find(|entry| entry["account"] == "acct-native")
        .unwrap();
    assert_eq!(native["status"], "failed");
    // Reader text never passes through; only the one fixed code does.
    assert_eq!(
        native["issues"],
        serde_json::json!(["credential_unavailable"])
    );
    // No credential or reference text appears anywhere in the report.
    let text = report.to_string();
    assert!(!text.contains(GLM_SECRET) && !text.contains(CLAUDE_SECRET));
    assert!(!text.contains("env:"));
}

/// Unknown script identities stay typed failures, never guesses.
#[tokio::test]
async fn unknown_collector_script_is_a_typed_failure() {
    let home = tempdir().unwrap();
    let accounts = [("acct-glm", "anthropic", "env:GLM_QUOTA_TOKEN")];
    registered(home.path(), &accounts);
    let catalog = catalog(
        vec![lua_provider(
            "glm-main",
            "codex_appserver",
            "https://open.bigmodel.cn",
            ("glm-5.3", None),
            &account("acct-glm"),
        )],
        &accounts,
    );
    let client = FakeHttp::new(vec![]);
    let mut backoff = AccountBackoff::default();
    let report = collect_provider_quota(
        home.path(),
        &catalog,
        64,
        &limits(),
        client,
        &FakeReader,
        &mut backoff,
    )
    .await
    .unwrap();
    assert!(!report["ok"].as_bool().unwrap());
    assert_eq!(
        report["results"][0]["issues"][0],
        "quota_collector_output_invalid_collector_unknown"
    );
    assert!(first_party("anthropic_usage").is_some());
}

/// A malformed official payload stays a typed failure and persists nothing.
#[tokio::test]
async fn malformed_payloads_fail_typed_without_persisting() {
    let home = tempdir().unwrap();
    let accounts = [("acct-glm", "anthropic", "env:GLM_QUOTA_TOKEN")];
    registered(home.path(), &accounts);
    let catalog = catalog(
        vec![lua_provider(
            "glm-main",
            "glm_quota",
            "https://open.bigmodel.cn",
            ("glm-5.3", None),
            &account("acct-glm"),
        )],
        &accounts,
    );
    let client = FakeHttp::new(vec![response(200, "{\"unrelated\":true}")]);
    let mut backoff = AccountBackoff::default();
    let report = collect_provider_quota(
        home.path(),
        &catalog,
        64,
        &limits(),
        client,
        &FakeReader,
        &mut backoff,
    )
    .await
    .unwrap();
    assert!(!report["ok"].as_bool().unwrap());
    assert_eq!(
        report["results"][0]["issues"][0],
        "quota_collector_script_failed"
    );
    assert!(stored(home.path(), "acct-glm").is_empty());
}

/// Contradictory alias bindings are rejected before credential or network
/// use, regardless of declaration order.
#[tokio::test]
async fn contradictory_alias_bindings_are_rejected_in_either_order() {
    let accounts = [("acct-glm", "anthropic", "env:GLM_QUOTA_TOKEN")];
    let bound = account("acct-glm");
    for providers in [
        vec![
            lua_provider(
                "glm-a",
                "glm_quota",
                "https://a.example",
                ("m", None),
                &bound,
            ),
            lua_provider(
                "glm-b",
                "glm_quota",
                "https://b.example",
                ("m", None),
                &bound,
            ),
        ],
        vec![
            lua_provider(
                "glm-b",
                "glm_quota",
                "https://b.example",
                ("m", None),
                &bound,
            ),
            lua_provider(
                "glm-a",
                "glm_quota",
                "https://a.example",
                ("m", None),
                &bound,
            ),
        ],
    ] {
        let catalog = catalog(providers, &accounts);
        let client = FakeHttp::new(vec![]);
        let mut backoff = AccountBackoff::default();
        let error = collect_provider_quota(
            tempdir().unwrap().path(),
            &catalog,
            64,
            &limits(),
            client.clone(),
            &FakeReader,
            &mut backoff,
        )
        .await
        .unwrap_err();
        assert!(error
            .to_string()
            .contains("contradictory collector bindings"));
        assert_eq!(client.issued(), 0, "rejected before any request");
    }
}

/// Eligible binding model subsets union across aliases; a disabled account's
/// bindings contribute nothing.
#[tokio::test]
async fn binding_subsets_union_across_aliases() {
    let home = tempdir().unwrap();
    let accounts = [
        ("acct-glm", "anthropic", "env:GLM_QUOTA_TOKEN"),
        ("acct-off", "anthropic", "env:GLM_OFF_TOKEN"),
    ];
    registered(home.path(), &accounts);
    let bound = account("acct-glm");
    let mut subset_provider = lua_provider(
        "glm-a",
        "glm_quota",
        "https://open.bigmodel.cn",
        ("alias-only", None),
        &bound,
    );
    subset_provider.models.push(ProviderModel {
        id: "unused".into(),
        native_model: None,
        params: Default::default(),
        allowed_params: Default::default(),
        recommendations: vec![],
        restrictions: vec![],
    });
    subset_provider.bindings[0].models = Some(vec!["alias-only".into()]);
    let catalog = catalog(
        vec![
            subset_provider,
            lua_provider(
                "glm-b",
                "glm_quota",
                "https://open.bigmodel.cn",
                ("inherit", None),
                &bound,
            ),
        ],
        &accounts,
    );
    let client = FakeHttp::new(vec![response(200, &glm_payload(10.0))]);
    let mut backoff = AccountBackoff::default();
    let report = collect_provider_quota(
        home.path(),
        &catalog,
        64,
        &limits(),
        client.clone(),
        &FakeReader,
        &mut backoff,
    )
    .await
    .unwrap();
    assert!(report["ok"].as_bool().unwrap());
    let conn = Connection::open(home.path().join("state.db")).unwrap();
    let membership: String = conn
        .query_row(
            "SELECT payload_json FROM capacity_samples WHERE account_id='acct-glm'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert!(membership.contains("alias-only") && membership.contains("inherit"));
    assert!(
        !membership.contains("unused"),
        "a non-subset provider model never enters the union"
    );
}

/// An endpoint Retry-After horizon is respected across aliases and rounds.
#[tokio::test]
async fn retry_after_horizon_suppresses_later_rounds() {
    let home = tempdir().unwrap();
    let accounts = [("acct-glm", "anthropic", "env:GLM_QUOTA_TOKEN")];
    registered(home.path(), &accounts);
    let bound = account("acct-glm");
    let catalog = catalog(
        vec![
            lua_provider(
                "glm-a",
                "glm_quota",
                "https://open.bigmodel.cn",
                ("m", None),
                &bound,
            ),
            lua_provider(
                "glm-b",
                "glm_quota",
                "https://open.bigmodel.cn",
                ("m", None),
                &bound,
            ),
        ],
        &accounts,
    );
    let throttled = QuotaHttpResponse {
        status: 429,
        headers: vec![("retry-after".into(), "120".into())],
        body: Vec::new(),
    };
    let client = FakeHttp::new(vec![throttled]);
    let mut backoff = AccountBackoff::default();
    let first = collect_provider_quota(
        home.path(),
        &catalog,
        64,
        &limits(),
        client.clone(),
        &FakeReader,
        &mut backoff,
    )
    .await
    .unwrap();
    assert_eq!(
        first["results"][0]["issues"][0],
        "quota_collector_rate_limited"
    );
    assert!(backoff.suppressed(&bound, "glm-quota", now_epoch() + 60.0));
    let second = collect_provider_quota(
        home.path(),
        &catalog,
        64,
        &limits(),
        client.clone(),
        &FakeReader,
        &mut backoff,
    )
    .await
    .unwrap();
    assert_eq!(second["results"][0]["issues"][0], "backoff");
    assert_eq!(client.issued(), 1, "aliases shared the suppression");
}

/// HTTP-date Retry-After values are honored, clamped to the 900 s cap, and
/// a throttle without a usable horizon stays the typed rate-limit outcome.
#[tokio::test]
async fn retry_after_http_date_and_bounds() {
    let bound = account("acct-glm");
    for (header, horizon) in [
        (
            Some(
                (chrono::Utc::now() + chrono::Duration::seconds(300))
                    .format("%a, %d %b %Y %H:%M:%S GMT")
                    .to_string(),
            ),
            Some(300.0),
        ),
        (Some("86400".to_owned()), Some(900.0)),
        (Some("soon".to_owned()), None),
        (None, None),
    ] {
        let home = tempdir().unwrap();
        let accounts = [("acct-glm", "anthropic", "env:GLM_QUOTA_TOKEN")];
        registered(home.path(), &accounts);
        let catalog = catalog(
            vec![lua_provider(
                "glm-a",
                "glm_quota",
                "https://open.bigmodel.cn",
                ("m", None),
                &bound,
            )],
            &accounts,
        );
        let throttled = QuotaHttpResponse {
            status: 503,
            headers: header
                .iter()
                .map(|value| ("retry-after".to_owned(), value.clone()))
                .collect(),
            body: Vec::new(),
        };
        let mut backoff = AccountBackoff::default();
        let start = now_epoch();
        let report = collect_provider_quota(
            home.path(),
            &catalog,
            64,
            &limits(),
            FakeHttp::new(vec![throttled]),
            &FakeReader,
            &mut backoff,
        )
        .await
        .unwrap();
        assert_eq!(
            report["results"][0]["issues"][0], "quota_collector_rate_limited",
            "{header:?}"
        );
        // The exponential default is 60 s; a parsed horizon extends it.
        let expected = horizon.unwrap_or(60.0);
        assert!(backoff.suppressed(&bound, "glm-quota", start + expected - 5.0));
        assert!(!backoff.suppressed(&bound, "glm-quota", start + expected + 5.0));
    }
}

/// A credential reader echoing secret text cannot reach the round report.
#[tokio::test]
async fn reader_errors_never_carry_secret_text() {
    /// Echoes the secret behind a plain or a trusted-looking prefix.
    struct LeakyReader(&'static str);
    impl CredentialReader for LeakyReader {
        fn read(&self, _reference: &CredentialRef) -> agent_run_domain::Result<String> {
            Err(agent_run_domain::Error::Validation(format!(
                "{}{GLM_SECRET}",
                self.0
            )))
        }
    }
    for prefix in [
        "boom ",
        "credential unavailable: ",
        "invalid ",
        "native login ",
    ] {
        reader_error_is_fixed(LeakyReader(prefix)).await;
    }
}

/// Runs one leaky reader through a real round and checks the fixed code.
async fn reader_error_is_fixed(reader: impl CredentialReader) {
    let home = tempdir().unwrap();
    let accounts = [("acct-glm", "anthropic", "env:GLM_QUOTA_TOKEN")];
    registered(home.path(), &accounts);
    let catalog = catalog(
        vec![lua_provider(
            "glm-main",
            "glm_quota",
            "https://open.bigmodel.cn",
            ("glm-5.3", None),
            &account("acct-glm"),
        )],
        &accounts,
    );
    let client = FakeHttp::new(vec![]);
    let mut backoff = AccountBackoff::default();
    let report = collect_provider_quota(
        home.path(),
        &catalog,
        64,
        &limits(),
        client,
        &reader,
        &mut backoff,
    )
    .await
    .unwrap();
    let text = report.to_string();
    assert!(
        !text.contains(GLM_SECRET),
        "report leaked reader text: {text}"
    );
    assert_eq!(
        report["results"][0]["issues"],
        serde_json::json!(["credential_unavailable"])
    );
}

/// The durable ledger survives the process boundary between polling rounds.
#[tokio::test]
async fn backoff_ledger_survives_restart_and_suppresses_real_rounds() {
    let home = tempdir().unwrap();
    let accounts = [("acct-glm", "anthropic", "env:GLM_QUOTA_TOKEN")];
    registered(home.path(), &accounts);
    let catalog = catalog(
        vec![lua_provider(
            "glm-main",
            "glm_quota",
            "https://open.bigmodel.cn",
            ("glm-5.3", None),
            &account("acct-glm"),
        )],
        &accounts,
    );
    // Round one fails in a "previous process" and persists its ledger.
    let client = FakeHttp::new(vec![]);
    let mut backoff = AccountBackoff::default();
    collect_provider_quota(
        home.path(),
        &catalog,
        64,
        &limits(),
        client.clone(),
        &FakeReader,
        &mut backoff,
    )
    .await
    .unwrap();
    backoff.save(home.path(), 0.0).unwrap();
    // A fresh process loads the ledger and skips the remote request.
    let mut restored = AccountBackoff::load(home.path());
    let client2 = FakeHttp::new(vec![]);
    let report = collect_provider_quota(
        home.path(),
        &catalog,
        64,
        &limits(),
        client2.clone(),
        &FakeReader,
        &mut restored,
    )
    .await
    .unwrap();
    assert_eq!(report["results"][0]["issues"][0], "backoff");
    assert_eq!(client2.issued(), 0);
}

/// The polling entry dispatches a schema-v2 home to the provider sources.
#[tokio::test]
async fn polling_round_dispatches_v2_homes_to_provider_sources() {
    let home = tempdir().unwrap();
    std::fs::write(
        home.path().join("config.toml"),
        format!(
            r#"
schema_version = 2
[harnesses.codex]
binary = "/bin/true"
home = "{home}/codex"
[harnesses.claude-code]
binary = "/bin/true"
home = "{home}/claude"
[providers.glm]
harness = "claude-code"
connection = {{ kind = "custom", endpoint = "https://open.bigmodel.cn/api", protocol = "messages" }}
auth_family = "anthropic"
limits_source = "lua"
collector = {{ script = "glm_quota", origins = ["https://open.bigmodel.cn"] }}
[[providers.glm.models]]
id = "glm-5.3"
[[providers.glm.bindings]]
label = "main"
account = "acct-glm"
"#,
            home = home.path().display()
        ),
    )
    .unwrap();
    let mut store = agent_run_store::Store::initialize(home.path()).unwrap();
    store
        .register_account(&AccountRecord {
            account_id: account("acct-glm"),
            auth_family: AuthFamily::from_str("anthropic").unwrap(),
            secret_ref: "env:GLM_QUOTA_TOKEN".parse().unwrap(),
            status: AccountStatus::Enabled,
        })
        .unwrap();
    let report = agent_run_core::capacity::sources::collect(home.path())
        .await
        .unwrap();
    let row = &report["results"][0];
    assert_eq!(row["account"], "acct-glm");
    assert_eq!(row["source"], "glm-quota");
    // Whether the live endpoint answers depends on the host; the dispatch
    // itself is the contract under test.
    assert!(row["status"].is_string());
}

/// Writes one private Claude credential document holding `token`.
fn claude_store(dir: &Path, token: &str) {
    use std::os::unix::fs::PermissionsExt;
    std::fs::create_dir_all(dir).unwrap();
    let file = dir.join(".credentials.json");
    std::fs::write(
        &file,
        format!(r#"{{"claudeAiOauth":{{"accessToken":"{token}"}}}}"#),
    )
    .unwrap();
    std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o600)).unwrap();
}

/// Native and named Claude logins resolve their own distinct stores, and a
/// missing named store never falls back to the default login.
#[test]
fn claude_native_and_named_logins_resolve_distinct_stores() {
    use agent_run_core::capacity::collectors::QuotaCredentialReader;
    let root = tempdir().unwrap();
    let app_home = root.path().join("agent-run");
    let native = root.path().join("native-claude");
    claude_store(&native, "native-token");
    claude_store(
        &app_home.join("accounts/claude/alpha/claude-config"),
        "alpha-token",
    );
    let reader = QuotaCredentialReader::new(app_home, native, true);
    let read = |reference: &str| reader.read(&CredentialRef::from_str(reference).unwrap());
    assert_eq!(read("native:claude-code").unwrap(), "native-token");
    assert_eq!(read("named:claude-code:alpha").unwrap(), "alpha-token");
    let missing = read("named:claude-code:beta").unwrap_err().to_string();
    assert!(!missing.contains("native-token") && !missing.contains("alpha-token"));
    assert!(read("native:codex").is_err());
    assert!(read("named:codex:alpha").is_err());
}

/// Keychain service names follow Claude Code's own per-directory rule.
#[test]
fn claude_keychain_service_is_directory_scoped() {
    use agent_run_core::capacity::quota_auth::keychain_service;
    assert_eq!(
        keychain_service(Path::new("/x/.claude"), false),
        "Claude Code-credentials"
    );
    let a = keychain_service(Path::new("/a/claude-config"), true);
    let b = keychain_service(Path::new("/b/claude-config"), true);
    assert!(a.starts_with("Claude Code-credentials-") && a.len() == 32);
    assert_ne!(a, b);
}

/// Runs one GLM round over `body` and returns the report.
async fn glm_round(home: &Path, body: &str) -> serde_json::Value {
    let accounts = [("acct-glm", "anthropic", "env:GLM_QUOTA_TOKEN")];
    registered(home, &accounts);
    let catalog = catalog(
        vec![lua_provider(
            "glm-main",
            "glm_quota",
            "https://api.z.ai",
            ("glm-5.3", None),
            &account("acct-glm"),
        )],
        &accounts,
    );
    let client = FakeHttp::new(vec![response(200, body)]);
    let mut backoff = AccountBackoff::default();
    collect_provider_quota(
        home,
        &catalog,
        64,
        &limits(),
        client,
        &FakeReader,
        &mut backoff,
    )
    .await
    .unwrap()
}

/// The live CREDIT_LIMIT shape yields distinct five-hour and weekly windows
/// with epoch-millisecond resets; inconsistent or unknown shapes fail typed.
#[tokio::test]
async fn glm_credit_limits_map_verified_windows() {
    let home = tempdir().unwrap();
    // Numbers mirror the live normalized canary: 23 % of five hours, 44 % of
    // the week, counts agreeing within rounding, `remaining` not a difference.
    let body = r#"{"code":200,"data":{"level":"pro","limits":[
        {"type":"CREDIT_LIMIT","unit":3,"number":5,"percentage":23,"usage":1000,"currentValue":233,"remaining":900,"nextResetTime":1900000000000},
        {"type":"CREDIT_LIMIT","unit":6,"number":1,"percentage":44,"usage":5000,"currentValue":2210,"remaining":1,"nextResetTime":1900300000000},
        {"type":"TIME_LIMIT","percentage":90,"currentValue":900,"usage":1000}
    ]}}"#;
    let report = glm_round(home.path(), body).await;
    assert_eq!(report["results"][0]["status"], "collected", "{report}");
    assert_eq!(
        stored(home.path(), "acct-glm"),
        vec![
            (
                "primary".to_owned(),
                "five_hour".to_owned(),
                Some(77.0),
                "glm-quota".to_owned()
            ),
            (
                "primary".to_owned(),
                "seven_day".to_owned(),
                Some(56.0),
                "glm-quota".to_owned()
            ),
        ]
    );
    let conn = Connection::open(home.path().join("state.db")).unwrap();
    let reset: f64 = conn
        .query_row(
            "SELECT reset_at FROM capacity_samples WHERE window='seven_day'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(reset, 1_900_300_000.0);
    for bad in [
        // percentage contradicts the absolute counts
        r#"{"data":{"limits":[{"type":"CREDIT_LIMIT","unit":3,"number":5,"percentage":10,"usage":100,"currentValue":50}]}}"#,
        // unverified window encoding is never guessed
        r#"{"data":{"limits":[{"type":"CREDIT_LIMIT","unit":5,"number":1,"percentage":10}]}}"#,
        // monthly MCP usage alone is not inference capacity
        r#"{"data":{"limits":[{"type":"TIME_LIMIT","percentage":10}]}}"#,
        // unknown limit families fail instead of being dropped
        r#"{"data":{"limits":[{"type":"TOKENS_LIMIT","percentage":10},{"type":"NEW_LIMIT","percentage":99}]}}"#,
        // resets outside the representable range
        r#"{"data":{"limits":[{"type":"CREDIT_LIMIT","unit":3,"number":5,"percentage":10,"nextResetTime":-5}]}}"#,
    ] {
        let home = tempdir().unwrap();
        let report = glm_round(home.path(), bad).await;
        assert_eq!(report["results"][0]["status"], "failed", "{bad}");
        assert!(stored(home.path(), "acct-glm").is_empty());
    }
}

/// Runs one Anthropic round over `body` for a provider serving two models.
async fn anthropic_round(home: &Path, body: &str) -> serde_json::Value {
    let accounts = [("acct-claude", "anthropic", "env:CLAUDE_QUOTA_TOKEN")];
    registered(home, &accounts);
    let mut provider = lua_provider(
        "claude-main",
        "anthropic_usage",
        "https://api.anthropic.com",
        ("claude-sonnet-5", None),
        &account("acct-claude"),
    );
    let mut fable = provider.models[0].clone();
    fable.id = "claude-fable-5-1".into();
    provider.models.push(fable);
    let catalog = catalog(vec![provider], &accounts);
    let client = FakeHttp::new(vec![response(200, body)]);
    let mut backoff = AccountBackoff::default();
    collect_provider_quota(
        home,
        &catalog,
        64,
        &limits(),
        client,
        &FakeReader,
        &mut backoff,
    )
    .await
    .unwrap()
}

/// A model-scoped weekly limit governs exactly the model it names; unknown
/// scope semantics fail typed instead of being ignored.
#[tokio::test]
async fn anthropic_scoped_limits_govern_only_their_models() {
    let home = tempdir().unwrap();
    // The live normalized envelope: general limits plus an active,
    // critical weekly limit scoped to one model by display name.
    let body = r#"{"five_hour":{"utilization":2},"seven_day_opus":{"utilization":1},"limits":[
        {"kind":"session","group":"session","is_active":false,"percent":2,"resets_at":"2030-01-01T00:00:00Z","scope":null,"severity":"normal"},
        {"kind":"weekly_all","group":"weekly","is_active":false,"percent":59,"resets_at":"2030-01-02T00:00:00Z","scope":null,"severity":"normal"},
        {"kind":"weekly_scoped","group":"weekly","is_active":true,"percent":96,"resets_at":"2030-01-03T00:00:00Z","scope":{"model":{"display_name":"Fable","id":null},"surface":null},"severity":"critical"}
    ]}"#;
    let report = anthropic_round(home.path(), body).await;
    assert_eq!(report["results"][0]["status"], "collected", "{report}");
    let rows = stored(home.path(), "acct-claude");
    assert_eq!(
        rows.iter()
            .map(|row| (row.0.as_str(), row.1.as_str(), row.2))
            .collect::<Vec<_>>(),
        vec![
            ("model:fable", "seven_day", Some(4.0)),
            ("primary", "five_hour", Some(98.0)),
            ("secondary", "seven_day", Some(41.0)),
        ]
    );
    let conn = Connection::open(home.path().join("state.db")).unwrap();
    let scoped: String = conn
        .query_row(
            "SELECT payload_json FROM capacity_samples WHERE lane='model:fable'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert!(scoped.contains("claude-fable-5-1") && !scoped.contains("claude-sonnet-5"));
    for bad in [
        r#"{"limits":[{"kind":"session","percent":2},{"kind":"weekly_new","percent":99}]}"#,
        r#"{"limits":[{"kind":"weekly_scoped","percent":99,"scope":{"model":{"display_name":"Fable"},"surface":"web"}}]}"#,
        r#"{"limits":[{"kind":"weekly_scoped","percent":99,"scope":{"model":{"id":null,"display_name":null}}}]}"#,
        r#"{"limits":[{"kind":"session","percent":2,"resets_at":"not-a-time"}]}"#,
        r#"{"limits":[{"kind":"session","percent":2,"scope":{"model":{"display_name":"Fable"}}}]}"#,
    ] {
        let home = tempdir().unwrap();
        let report = anthropic_round(home.path(), bad).await;
        assert_eq!(report["results"][0]["status"], "failed", "{bad}");
        assert!(stored(home.path(), "acct-claude").is_empty());
    }
}

/// A custom script whose single window reports `remaining` for all models.
fn custom_script(remaining: u32) -> String {
    format!(
        "collect = function(ctx)\n  local m = {{}}\n  for n, _ in pairs(ctx.models) do m[#m + 1] = n end\n  return {{ version = 1, windows = {{ {{ pool = \"primary\", window = \"five_hour\", models = m, remaining_percent = {remaining}, observed_at = ctx.now }} }} }}\nend\n"
    )
}

/// Runs one round of custom id `team_quota` bound to `file` for `acct`.
async fn custom_round(home: &Path, acct: &str, file: &Path) -> serde_json::Value {
    let accounts = [(acct, "anthropic", "env:GLM_QUOTA_TOKEN")];
    if !home.join("state.db").exists() {
        registered(home, &accounts);
    }
    let mut provider = lua_provider(
        "team",
        "team_quota",
        "https://quota.example",
        ("model-a", None),
        &account(acct),
    );
    provider.collector = Some(CollectorBinding {
        script: "team_quota".into(),
        origins: vec!["https://quota.example/".into()],
        script_file: Some(file.to_path_buf()),
        auth: Some(agent_run_domain::catalog::CredentialPlacement::BearerAuthorization),
    });
    let catalog = catalog(vec![provider], &accounts);
    let mut backoff = AccountBackoff::default();
    collect_provider_quota(
        home,
        &catalog,
        64,
        &limits(),
        FakeHttp::new(vec![]),
        &FakeReader,
        &mut backoff,
    )
    .await
    .unwrap()
}

/// One custom id over two files never shares code; oversized replacements
/// are refused before allocation, and the durable last-valid copy serves a
/// fresh process whose file went bad.
#[tokio::test]
async fn custom_scripts_bind_to_their_full_source_identity() {
    use sha2::{Digest, Sha256};
    let root = tempdir().unwrap();
    let (file_a, file_b) = (root.path().join("a.lua"), root.path().join("b.lua"));
    std::fs::write(&file_a, custom_script(11)).unwrap();
    std::fs::write(&file_b, custom_script(22)).unwrap();
    let (home_a, home_b) = (tempdir().unwrap(), tempdir().unwrap());
    custom_round(home_a.path(), "acct-a", &file_a).await;
    custom_round(home_b.path(), "acct-b", &file_b).await;
    assert_eq!(stored(home_a.path(), "acct-a")[0].2, Some(11.0));
    assert_eq!(stored(home_b.path(), "acct-b")[0].2, Some(22.0));
    // An oversized replacement keeps the retained revision in this process.
    std::fs::write(&file_a, "-- ".repeat(200 * 1024)).unwrap();
    let report = custom_round(home_a.path(), "acct-a", &file_a).await;
    assert_eq!(report["results"][0]["status"], "collected", "{report}");
    // A fresh identity whose file is bad uses the durable copy a previous
    // polling round left behind.
    let file_c = root.path().join("c.lua");
    std::fs::write(&file_c, "collect = ").unwrap();
    let identity = format!(
        "team_quota\n{}",
        std::fs::canonicalize(&file_c).unwrap().display()
    );
    let digest: String = Sha256::digest(identity.as_bytes())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    let home_c = tempdir().unwrap();
    std::fs::create_dir_all(home_c.path().join("capacity/scripts")).unwrap();
    std::fs::write(
        home_c.path().join(format!("capacity/scripts/{digest}.lua")),
        custom_script(33),
    )
    .unwrap();
    let report = custom_round(home_c.path(), "acct-c", &file_c).await;
    assert_eq!(report["results"][0]["status"], "collected", "{report}");
    assert_eq!(stored(home_c.path(), "acct-c")[0].2, Some(33.0));
    // Without any retained revision a bad file is a typed failure.
    let home_d = tempdir().unwrap();
    let file_d = root.path().join("d.lua");
    std::fs::write(&file_d, "collect = ").unwrap();
    let report = custom_round(home_d.path(), "acct-d", &file_d).await;
    assert_eq!(
        report["results"][0]["issues"][0],
        "quota_collector_output_invalid_collector_script_unavailable"
    );
}
