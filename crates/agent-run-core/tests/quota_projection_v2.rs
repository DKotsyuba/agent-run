//! Synthetic schema-2 Quota read projections; no network, credentials or engines.
use agent_run_core::{capacity::quota_projection, quota_client::Snapshot};
use agent_run_domain::{
    CapacityOrderQuery, ModelsQuery,
    catalog::{AccountRecord, AccountStatus},
};
use serde_json::{Value, json};
use std::path::Path;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::UnixListener,
};

const KNOWN: &str = include_str!("fixtures/quota-client-v1/snapshot-v2.json");
const UNKNOWN: &str = include_str!("fixtures/quota-client-v1/unknown-snapshot-v2.json");
const EXHAUSTED: &str = include_str!("fixtures/quota-client-v1/exhausted-snapshot-v2.json");
const OVERFLOW: &str = include_str!("fixtures/quota-client-v1/overflow-snapshot-v2.json");
const AT: f64 = 10002.0;

/// Builds an isolated catalog with case-sensitive providers and private fake accounts.
fn home() -> tempfile::TempDir {
    let base = std::env::var_os("AGENT_RUN_TEST_TMP")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(std::env::temp_dir);
    let home = tempfile::Builder::new()
        .prefix("quota-projection-v2-")
        .tempdir_in(base)
        .unwrap();
    agent_run_store::fixture::initialize(home.path()).unwrap();
    std::fs::create_dir_all(home.path().join("profiles")).unwrap();
    std::fs::write(home.path().join("profiles/review.md"), "+++\nrevision='1'\nwrite=false\nnetwork=false\nallow_external_read_roots=false\nskills=[]\nmcp=[]\nrequired_constraints=[]\n+++\nReview.\n").unwrap();
    let root = home.path().display();
    let mut config = format!(
        "schema_version=2\n[harnesses.codex]\nbinary='/usr/bin/true'\nhome='{root}/codex'\n[harnesses.claude-code]\nbinary='/usr/bin/true'\nhome='{root}/claude'\n"
    );
    for (provider, account, multiplier) in [
        ("Claude", "acct-a", 1.5),
        ("claude", "acct-b", 1.0),
        ("1gateway", "acct-c", 1.0),
    ] {
        config.push_str(&format!("[providers.{provider}]\nharness='codex'\nconnection={{kind='native'}}\nauth_family='openai'\nlimits_source='none'\npriority_multiplier={multiplier}\n[[providers.{provider}.models]]\nid='model-a'\n[[providers.{provider}.bindings]]\nlabel='private-{account}'\naccount='{account}'\npriority_multiplier=2.0\n"));
    }
    std::fs::write(home.path().join("config.toml"), config).unwrap();
    let mut store = agent_run_store::fixture::open(home.path()).unwrap();
    for id in ["acct-a", "acct-b", "acct-c", "acct-extra"] {
        store
            .register_account(&AccountRecord {
                account_id: id.parse().unwrap(),
                auth_family: "openai".parse().unwrap(),
                secret_ref: format!("named:codex:{id}").parse().unwrap(),
                status: AccountStatus::Enabled,
            })
            .unwrap();
    }
    drop(store);
    home
}

/// One source account retaining Quota's reported state, windows and identities.
fn account(id: &str, fixture: &str, provider: &str) -> Value {
    let raw: Value = serde_json::from_str(&fixture.replace("acct-test", id)).unwrap();
    let mut account = raw["result"]["accounts"][0].clone();
    account["models"][0]["provider"] = json!(provider);
    for window in account["windows"].as_array_mut().unwrap() {
        window["runtime"] = json!(provider);
    }
    account
}

/// Known A, unknown B, and an active no-reset C latch despite its stale window.
fn snapshot() -> Snapshot {
    let mut raw: Value = serde_json::from_str(UNKNOWN).unwrap();
    let mut c = account("acct-c", EXHAUSTED, "1gateway");
    c["models"][0]["state_validity"] = json!({"kind":"latched_no_reset"});
    c["models"][0]["remaining_percent"] = Value::Null;
    c["models"][0]["burn_percent_per_hour"] = Value::Null;
    c["models"][0]["reset_at"] = Value::Null;
    c["models"][0]["exhausted_until"] = Value::Null;
    c["models"][0]["risk"] = json!("unknown");
    c["latches"][0]["reset_at"] = Value::Null;
    let forecast = &mut c["windows"][0]["forecast"];
    forecast["known"] = json!(false);
    for key in [
        "remaining_percent",
        "reset_at",
        "observed_at",
        "burn_percent_per_hour",
        "sustainable_percent_per_hour",
        "burn_span_seconds",
    ] {
        forecast[key] = Value::Null;
    }
    forecast["warmup"] = json!(true);
    forecast["risk"] = json!("unknown");
    c["windows"][0]["evaluation"] = Value::Null;
    raw["result"]["evaluated_at"] = json!(AT);
    raw["result"]["accounts"] = json!([
        account("acct-a", KNOWN, "Claude"),
        account("acct-b", UNKNOWN, "claude"),
        c,
        account("acct-extra", KNOWN, "Other")
    ]);
    serde_json::from_value(raw["result"].clone()).unwrap()
}

/// Loads the immutable test catalog, preserving its exact-byte revision.
fn config(home: &Path) -> (agent_run_config::provider_config::ProviderConfig, String) {
    agent_run_config::provider_config::ProviderConfig::load(home).unwrap()
}

/// Returns one provider/model's compatibility quota object.
fn standing<'a>(value: &'a Value, provider: &str) -> &'a Value {
    &value["providers"]
        .as_array()
        .unwrap()
        .iter()
        .find(|entry| entry["provider"] == provider)
        .unwrap()["models"][0]["quota"]
}

#[test]
fn quota_projection_v2_preserves_schema_fields_and_explicit_partial_coverage() {
    let home = home();
    let store = agent_run_store::fixture::open(home.path()).unwrap();
    let (config, revision) = config(home.path());
    let source = snapshot();
    let limits =
        quota_projection::limits_at(&store, &config, &revision, Some(&source), AT).unwrap();
    assert_eq!(limits["observed_at"], AT);
    assert_eq!(limits["ranking"]["ranked_at"], AT);
    assert_eq!(limits["coverage"]["state"], "partial");
    assert_eq!(limits["coverage"]["incomplete"], true);
    assert_eq!(limits["coverage"]["evaluated_at"], AT);
    assert_eq!(limits["ranking"]["quota_coverage"]["evaluated_at"], AT);
    assert!(!limits["coverage"].to_string().contains("acct-"));
    let items = limits["items"].as_array().unwrap();
    assert_eq!(items.len(), 2);
    let a = items
        .iter()
        .find(|item| item["account"] == "acct-a")
        .unwrap();
    assert_eq!(a["known"], true);
    assert_eq!(a["remaining_percent"], 80.0);
    let c = items
        .iter()
        .find(|item| item["account"] == "acct-c")
        .unwrap();
    assert_eq!(c["known"], false);
    assert!(c["remaining_percent"].is_null());
    assert_eq!(c["reset_at"], 20000.0);
    assert!(!limits.to_string().contains("acct-extra"));
    assert_eq!(
        standing(&limits["ranking"], "Claude")["status"],
        "available"
    );
    assert_eq!(standing(&limits["ranking"], "claude")["status"], "unknown");
    assert_eq!(
        standing(&limits["ranking"], "1gateway")["status"],
        "exhausted"
    );
    assert_eq!(
        limits["ranking"]["capacity_revision"],
        store.quota_capacity_revision().unwrap()
    );
}

#[test]
fn quota_projection_v2_models_order_and_guide_keep_filters_and_privacy() {
    let home = home();
    let store = agent_run_store::fixture::open(home.path()).unwrap();
    let (config, revision) = config(home.path());
    let source = snapshot();
    let query = ModelsQuery::default();
    let models = quota_projection::models_at(
        &store,
        home.path(),
        &config,
        &revision,
        &query,
        Some(&source),
        AT + 1.0,
    )
    .unwrap();
    for key in [
        "schema_version",
        "config_revision",
        "capacity_revision",
        "ranked_at",
        "roles_sha256",
        "profiles",
        "providers",
    ] {
        assert!(models.get(key).is_some(), "missing {key}");
    }
    assert_eq!(standing(&models, "Claude")["status"], "available");
    assert_eq!(standing(&models, "claude")["status"], "unknown");
    assert_eq!(models["ranked_at"], AT + 1.0);
    assert_eq!(models["quota_coverage"]["evaluated_at"], AT);
    assert_eq!(models["quota_coverage"]["incomplete"], true);
    assert!(!models.to_string().contains("acct-"));
    assert!(!models.to_string().contains("named:codex"));
    let filtered = quota_projection::models_at(
        &store,
        home.path(),
        &config,
        &revision,
        &ModelsQuery {
            provider: Some("Claude".into()),
            ..ModelsQuery::default()
        },
        Some(&source),
        AT + 1.0,
    )
    .unwrap();
    assert_eq!(filtered["providers"].as_array().unwrap().len(), 1);
    assert_eq!(filtered["providers"][0]["provider"], "Claude");
    let order = quota_projection::order_at(
        &store,
        &config,
        &revision,
        &CapacityOrderQuery::default(),
        Some(&source),
        AT + 1.0,
    )
    .unwrap();
    assert!(!order.to_string().contains("acct-"));
    assert_eq!(order["ranked_at"], AT + 1.0);
    assert_eq!(order["quota_coverage"]["evaluated_at"], AT);
    assert_eq!(order["providers"][0]["provider"], "Claude");
    let priority = standing(&order, "Claude")["best_priority"]
        .as_f64()
        .unwrap();
    assert!((priority - 3.2).abs() < 1e-9);
    let guide = quota_projection::delegation_guide_at(
        &store,
        home.path(),
        &config,
        &revision,
        &query,
        Some(&source),
        AT,
    )
    .unwrap();
    let text = guide.as_str().unwrap();
    assert!(text.contains("Claude"));
    assert!(!text.contains("acct-"));
    assert!(!text.contains("named:codex"));
}

#[test]
fn quota_projection_v2_outage_expiry_rollback_and_local_cooldown_are_unknown() {
    let home = home();
    let store = agent_run_store::fixture::open(home.path()).unwrap();
    let (config, revision) = config(home.path());
    let source = snapshot();
    let outage = quota_projection::limits_at(&store, &config, &revision, None, AT).unwrap();
    assert_eq!(outage["coverage"]["state"], "unknown");
    assert_eq!(outage["coverage"]["incomplete"], true);
    assert!(outage["items"].as_array().unwrap().is_empty());
    assert_eq!(standing(&outage["ranking"], "Claude")["status"], "unknown");
    assert_eq!(outage["ranking"]["quota_coverage"]["state"], "unknown");
    assert_eq!(outage["ranking"]["quota_coverage"]["incomplete"], true);
    assert!(outage["ranking"]["quota_coverage"]["evaluated_at"].is_null());
    let rollback =
        quota_projection::limits_at(&store, &config, &revision, Some(&source), AT - 1.0).unwrap();
    assert_eq!(rollback["coverage"]["incomplete"], true);
    assert_eq!(
        standing(&rollback["ranking"], "1gateway")["status"],
        "unknown"
    );
    let expired =
        quota_projection::limits_at(&store, &config, &revision, Some(&source), 10900.0).unwrap();
    assert_eq!(standing(&expired["ranking"], "Claude")["status"], "unknown");
    assert_eq!(
        standing(&expired["ranking"], "1gateway")["status"],
        "exhausted"
    );
    let boundary = expired["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["account"] == "acct-a")
        .unwrap();
    assert_eq!(boundary["known"], true);
    assert_eq!(boundary["remaining_percent"], 80.0);
    let after =
        quota_projection::limits_at(&store, &config, &revision, Some(&source), 10900.001).unwrap();
    let after = after["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["account"] == "acct-a")
        .unwrap();
    assert_eq!(after["known"], false);
    assert!(after["remaining_percent"].is_null());
    store.conn.execute("INSERT INTO binding_cooldowns(provider_id,account_id,reason,observed_at,blocked_until,attempt_id) VALUES('Claude','acct-a','billing_unavailable',?1,?2,'synthetic')", rusqlite::params![AT, AT + 100.0]).unwrap();
    let order = quota_projection::order_at(
        &store,
        &config,
        &revision,
        &CapacityOrderQuery::default(),
        Some(&source),
        AT,
    )
    .unwrap();
    assert_eq!(standing(&order, "Claude")["status"], "no_eligible_account");
    assert!(matches!(
        quota_projection::legacy_order_unavailable(),
        Err(agent_run_domain::Error::Unsupported(_))
    ));
}

#[test]
fn quota_projection_v2_ineligible_missing_binding_is_incomplete() {
    let home = home();
    let mut store = agent_run_store::fixture::open(home.path()).unwrap();
    let (mut config, revision) = config(home.path());
    config
        .providers
        .retain(|provider, _| provider.as_str() == "Claude");
    // Quota reports a complete collection for a different account. That says
    // nothing about this configuration's absent account, even when disabled.
    let mut raw: Value = serde_json::from_str(KNOWN).unwrap();
    raw["result"]["accounts"] = json!([account("acct-extra", KNOWN, "Other")]);
    raw["result"]["evaluated_at"] = json!(AT);
    let missing: Snapshot = serde_json::from_value(raw["result"].clone()).unwrap();
    store.disable_account(&"acct-a".parse().unwrap()).unwrap();
    let check = |store: &agent_run_store::Store<'_>| {
        let limits =
            quota_projection::limits_at(store, &config, &revision, Some(&missing), AT).unwrap();
        assert_eq!(limits["coverage"]["state"], "complete");
        assert_eq!(limits["coverage"]["incomplete"], true);
        assert!(limits["items"].as_array().unwrap().is_empty());
        assert_eq!(
            standing(&limits["ranking"], "Claude")["status"],
            "no_eligible_account"
        );
        for value in [
            quota_projection::models_at(
                store,
                home.path(),
                &config,
                &revision,
                &ModelsQuery::default(),
                Some(&missing),
                AT,
            )
            .unwrap(),
            quota_projection::order_at(
                store,
                &config,
                &revision,
                &CapacityOrderQuery::default(),
                Some(&missing),
                AT,
            )
            .unwrap(),
        ] {
            assert_eq!(value["quota_coverage"]["incomplete"], true);
        }
    };
    check(&store);
    // Exercise cooldown separately with the account enabled again.
    store
        .conn
        .execute(
            "UPDATE provider_accounts SET status='enabled' WHERE account_id='acct-a'",
            [],
        )
        .unwrap();
    store.conn.execute("INSERT INTO binding_cooldowns(provider_id,account_id,reason,observed_at,blocked_until,attempt_id) VALUES('Claude','acct-a','billing_unavailable',?1,?2,'synthetic')", rusqlite::params![AT, AT + 100.0]).unwrap();
    check(&store);

    // A completely empty local scope is a proven empty view, not an outage.
    config.providers.clear();
    let empty =
        quota_projection::limits_at(&store, &config, &revision, Some(&missing), AT).unwrap();
    assert!(empty["items"].as_array().unwrap().is_empty());
    assert_eq!(empty["coverage"]["incomplete"], false);
    let unavailable = quota_projection::limits_at(&store, &config, &revision, None, AT).unwrap();
    assert_eq!(unavailable["coverage"]["incomplete"], true);
}

/// Public dispatch takes exactly one real decoded Quota response per read and
/// keeps partial and unavailable responses distinct without reading old rows.
#[tokio::test]
async fn quota_projection_v2_public_reads_fetch_once_and_report_outage() {
    let home = home();
    let base = std::env::var_os("AGENT_RUN_TEST_SOCKET_TMP")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(std::env::temp_dir);
    let dir = tempfile::Builder::new()
        .prefix("qp-public-")
        .tempdir_in(base)
        .unwrap();
    let socket = dir.path().join("quota.sock");
    let listener = UnixListener::bind(&socket).unwrap();
    let path = home.path().join("config.toml");
    let mut config = std::fs::read_to_string(&path).unwrap();
    config.push_str(&format!(
        "\n[capacity.quota]\nsocket={}\n",
        toml::Value::String(socket.to_string_lossy().into_owned())
    ));
    std::fs::write(path, config).unwrap();
    let at = agent_run_core::domain::now();
    let until = at + 900.0;
    let mut response: Value = serde_json::from_str(KNOWN).unwrap();
    let mut a = account("acct-a", KNOWN, "Claude");
    a["models"][0]["state_validity"] = json!({"kind":"until","at":until});
    a["models"][0]["newest_observed_at"] = json!(at);
    a["models"][0]["reset_at"] = json!(until);
    a["windows"][0]["observed_at"] = json!(at);
    a["windows"][0]["valid_until"] = json!(until);
    a["windows"][0]["last_reset_at"] = json!(until);
    a["windows"][0]["forecast"]["observed_at"] = json!(at);
    a["windows"][0]["forecast"]["reset_at"] = json!(until);
    a["windows"][0]["evaluation"]["reset_at"] = json!(until);
    a["advice"][0]["reset_at"] = json!(until);
    response["result"]["evaluated_at"] = json!(at);
    response["result"]["coverage"]["state"] = json!("partial");
    response["result"]["accounts"] = json!([a]);
    let mut bytes = serde_json::to_vec(&response).unwrap();
    bytes.push(b'\n');
    let server = tokio::spawn(async move {
        for _ in 0..4 {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            stream.read_to_end(&mut request).await.unwrap();
            assert_eq!(
                serde_json::from_slice::<Value>(&request).unwrap()["accounts"],
                json!([])
            );
            stream.write_all(&bytes).await.unwrap();
        }
    });
    let service = agent_run_core::service::Service::new(home.path().to_path_buf());
    let models = agent_run_core::dispatch::call(&service, "models", json!({}))
        .await
        .unwrap();
    assert_eq!(standing(&models, "Claude")["status"], "available");
    assert_eq!(models["quota_coverage"]["incomplete"], true);
    let limits = agent_run_core::dispatch::call(&service, "limits", json!({}))
        .await
        .unwrap();
    assert_eq!(limits["items"][0]["remaining_percent"], 80.0);
    assert_eq!(limits["coverage"]["state"], "partial");
    let order = agent_run_core::dispatch::call(&service, "capacity_order", json!({}))
        .await
        .unwrap();
    assert_eq!(standing(&order, "Claude")["status"], "available");
    let guide = agent_run_core::dispatch::call(&service, "delegation_guide", json!({}))
        .await
        .unwrap();
    assert!(guide.as_str().unwrap().contains("Claude"));
    server.await.unwrap();
    let unavailable = agent_run_core::dispatch::call(&service, "limits", json!({}))
        .await
        .unwrap();
    assert_eq!(unavailable["coverage"]["state"], "unknown");
    assert_eq!(unavailable["coverage"]["incomplete"], true);
    assert!(unavailable["items"].as_array().unwrap().is_empty());
}

#[test]
fn quota_projection_v2_fresh_raw_value_survives_unknown_model_evaluation() {
    let home = home();
    let store = agent_run_store::fixture::open(home.path()).unwrap();
    let (config, revision) = config(home.path());
    let mut source = snapshot();
    let a = source
        .accounts
        .iter_mut()
        .find(|account| account.account == "acct-a")
        .unwrap();
    let model = &mut a.models[0];
    model.state = agent_run_core::quota_client::ModelState::Unknown;
    model.state_validity = agent_run_core::quota_client::StateValidity::Unknown;
    model.quota_score = None;
    model.limiting_window = None;
    model.remaining_percent = None;
    model.burn_percent_per_hour = None;
    model.reset_at = None;
    model.risk = agent_run_core::quota_client::Risk::Unknown;
    model.evidence = agent_run_core::quota_client::Evidence::Stale;
    a.windows[0].evaluation = None;
    a.windows[0].forecast.burn_percent_per_hour = None;
    a.windows[0].forecast.sustainable_percent_per_hour = None;
    let result =
        quota_projection::limits_at(&store, &config, &revision, Some(&source), AT).unwrap();
    let item = result["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["account"] == "acct-a")
        .unwrap();
    assert_eq!(item["known"], true);
    assert_eq!(item["remaining_percent"], 80.0);
    assert_eq!(standing(&result["ranking"], "Claude")["status"], "unknown");
    assert_eq!(result["coverage"]["incomplete"], true);
}

#[tokio::test]
async fn quota_projection_v2_real_overflow_fixture_keeps_healthy_and_latched_neighbors() {
    let home = home();
    let path = home.path().join("config.toml");
    let mut text = std::fs::read_to_string(&path).unwrap();
    text.push_str("\n[providers.codex]\nharness='codex'\nconnection={kind='native'}\nauth_family='openai'\nlimits_source='none'\n[[providers.codex.models]]\nid='a'\n[[providers.codex.bindings]]\nlabel='bad'\naccount='acct-bad'\n");
    std::fs::write(path, text).unwrap();
    let mut store = agent_run_store::fixture::open(home.path()).unwrap();
    store
        .register_account(&AccountRecord {
            account_id: "acct-bad".parse().unwrap(),
            auth_family: "openai".parse().unwrap(),
            secret_ref: "named:codex:acct-bad".parse().unwrap(),
            status: AccountStatus::Enabled,
        })
        .unwrap();
    let (config, revision) = config(home.path());
    let mut response: Value = serde_json::from_str(OVERFLOW).unwrap();
    let at = response["result"]["evaluated_at"].as_f64().unwrap();
    let mut healthy = account("acct-a", KNOWN, "Claude");
    healthy["models"][0]["state_validity"] = json!({"kind":"until","at":1000.0});
    healthy["models"][0]["reset_at"] = json!(1000.0);
    healthy["models"][0]["newest_observed_at"] = json!(at);
    healthy["windows"][0]["observed_at"] = json!(at);
    healthy["windows"][0]["valid_until"] = json!(1000.0);
    healthy["windows"][0]["last_reset_at"] = json!(1000.0);
    healthy["windows"][0]["forecast"]["observed_at"] = json!(at);
    healthy["windows"][0]["forecast"]["reset_at"] = json!(1000.0);
    healthy["windows"][0]["forecast"]["sustainable_percent_per_hour"] = json!(288.0);
    healthy["windows"][0]["evaluation"]["reset_at"] = json!(1000.0);
    healthy["advice"][0]["reset_at"] = json!(1000.0);
    let mut held = account("acct-c", EXHAUSTED, "1gateway");
    held["models"][0]["state_validity"] = json!({"kind":"latched_no_reset"});
    held["models"][0]["reset_at"] = Value::Null;
    held["models"][0]["exhausted_until"] = Value::Null;
    held["models"][0]["newest_observed_at"] = json!(at);
    held["models"][0]["burn_percent_per_hour"] = Value::Null;
    held["windows"][0]["observed_at"] = json!(at);
    held["windows"][0]["valid_until"] = json!(1000.0);
    held["windows"][0]["last_reset_at"] = json!(1000.0);
    held["windows"][0]["forecast"]["observed_at"] = json!(at);
    held["windows"][0]["forecast"]["reset_at"] = json!(1000.0);
    held["windows"][0]["forecast"]["warmup"] = json!(true);
    held["windows"][0]["forecast"]["burn_percent_per_hour"] = Value::Null;
    held["windows"][0]["forecast"]["burn_span_seconds"] = Value::Null;
    held["windows"][0]["evaluation"]["reset_at"] = json!(1000.0);
    held["windows"][0]["evaluation"]["burn_percent_per_hour"] = Value::Null;
    held["windows"][0]["evaluation"]["burn_span_seconds"] = Value::Null;
    held["windows"][0]["evaluation"]["marker"] = json!("warmup");
    held["latches"][0]["observed_at"] = json!(at);
    held["latches"][0]["reset_at"] = Value::Null;
    held["advice"][0]["warmup"] = json!(true);
    held["advice"][0]["reset_at"] = json!(1000.0);
    response["result"]["accounts"]
        .as_array_mut()
        .unwrap()
        .extend([healthy, held]);
    let mut bytes = serde_json::to_vec(&response).unwrap();
    bytes.push(b'\n');
    let base = std::env::var_os("AGENT_RUN_TEST_SOCKET_TMP")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(std::env::temp_dir);
    let dir = tempfile::Builder::new()
        .prefix("qp-overflow-")
        .tempdir_in(base)
        .unwrap();
    let socket = dir.path().join("quota.sock");
    let listener = UnixListener::bind(&socket).unwrap();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut request = Vec::new();
        stream.read_to_end(&mut request).await.unwrap();
        stream.write_all(&bytes).await.unwrap();
    });
    let decoded = agent_run_core::quota_client::snapshot(&socket, None, &[], None)
        .await
        .unwrap();
    server.await.unwrap();
    let result =
        quota_projection::limits_at(&store, &config, &revision, Some(&decoded), at).unwrap();
    let raw = result["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["account"] == "acct-bad")
        .unwrap();
    assert_eq!(raw["known"], true);
    assert_eq!(raw["remaining_percent"], 80.0);
    assert_eq!(standing(&result["ranking"], "codex")["status"], "unknown");
    assert_eq!(
        standing(&result["ranking"], "Claude")["status"],
        "available"
    );
    assert_eq!(
        standing(&result["ranking"], "1gateway")["status"],
        "exhausted"
    );
    assert_eq!(result["coverage"]["incomplete"], true);
}

#[test]
fn quota_projection_v2_marks_legacy_row_limit_as_incomplete() {
    let home = home();
    let store = agent_run_store::fixture::open(home.path()).unwrap();
    let (mut config, revision) = config(home.path());
    config
        .providers
        .retain(|provider, _| provider.as_str() == "Claude");
    let mut raw: Value = serde_json::from_str(KNOWN).unwrap();
    let mut view = account("acct-a", KNOWN, "Claude");
    let template = view["windows"][0].clone();
    let windows: Vec<Value> = (0..1001)
        .map(|index| {
            let mut window = template.clone();
            let name = format!("w-{index:04}");
            window["window"] = json!(name);
            window["forecast"]["key"]["window"] = json!(name);
            window["evaluation"]["key"]["window"] = json!(name);
            window
        })
        .collect();
    view["windows"] = json!(windows);
    view["models"][0]["limiting_window"]["window"] = json!("w-0000");
    view["advice"][0]["key"]["window"] = json!("w-0000");
    raw["result"]["accounts"] = json!([view]);
    raw["result"]["evaluated_at"] = json!(AT);
    let mut exactly = raw.clone();
    exactly["result"]["accounts"][0]["windows"]
        .as_array_mut()
        .unwrap()
        .pop();
    let exactly: Snapshot = serde_json::from_value(exactly["result"].clone()).unwrap();
    let untruncated =
        quota_projection::limits_at(&store, &config, &revision, Some(&exactly), AT).unwrap();
    assert_eq!(untruncated["items"].as_array().unwrap().len(), 1000);
    assert_eq!(untruncated["coverage"]["incomplete"], false);
    let source: Snapshot = serde_json::from_value(raw["result"].clone()).unwrap();
    let value = quota_projection::limits_at(&store, &config, &revision, Some(&source), AT).unwrap();
    assert_eq!(value["items"].as_array().unwrap().len(), 1000);
    assert_eq!(value["coverage"]["incomplete"], true);
}

#[test]
fn quota_projection_v2_does_not_apply_admission_pool_limit_to_reads() {
    let home = home();
    let store = agent_run_store::fixture::open(home.path()).unwrap();
    let (config, revision) = config(home.path());
    let mut raw: Value = serde_json::from_str(KNOWN).unwrap();
    let mut view = account("acct-a", KNOWN, "Claude");
    let template = view["windows"][0].clone();
    for index in 1..33 {
        let lane = format!("pool-{index}");
        let pool = format!("acct-a::{lane}");
        let mut window = template.clone();
        window["lane"] = json!(lane);
        window["pool"] = json!(pool);
        window["forecast"]["key"]["lane"] = json!(lane);
        window["evaluation"]["key"]["lane"] = json!(lane);
        view["windows"].as_array_mut().unwrap().push(window);
        view["models"][0]["pools"]
            .as_array_mut()
            .unwrap()
            .push(json!(pool));
    }
    raw["result"]["accounts"] = json!([view]);
    raw["result"]["evaluated_at"] = json!(AT);
    let source: Snapshot = serde_json::from_value(raw["result"].clone()).unwrap();
    let value = quota_projection::limits_at(&store, &config, &revision, Some(&source), AT).unwrap();
    assert_eq!(value["items"].as_array().unwrap().len(), 33);
    assert_eq!(standing(&value["ranking"], "Claude")["status"], "available");
}
