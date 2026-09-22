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
    assert_eq!(
        native["issues"][0],
        "native login remains owned by the harness"
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
    assert_eq!(report["results"][0]["issues"][0], "collector_unknown");
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
