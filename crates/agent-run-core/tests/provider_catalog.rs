//! Public schema-2 provider catalog (`models`) and provider `capacity_order`
//! through the real service and dispatch: exact filters, privacy, revision
//! consistency, read-only semantics, and live recommendation edits.

mod common;
use agent_run_core::{dispatch, service::Service};
use agent_run_domain::{
    CapacityOrderQuery, ModelsQuery,
    catalog::{AccountRecord, AccountStatus},
};
use serde_json::{Value, json};
use std::{fs, path::Path};

/// Writes the v2 config with `recommendation` on the codex provider and a
/// distinctive account label so privacy assertions cannot match ordinary prose.
fn write_config(root: &Path, recommendation: &str) {
    fs::write(
        root.join("config.toml"),
        format!(
            r#"schema_version = 2
[harnesses.codex]
binary = "/bin/true"
home = "{root}/codex"
[harnesses.claude-code]
binary = "/bin/true"
home = "{root}/claude"
[providers.codex]
harness = "codex"
connection = {{ kind = "native" }}
auth_family = "openai"
limits_source = "none"
recommendations = ["{recommendation}"]
[[providers.codex.models]]
id = "gpt-main"
native_model = "gpt-native"
params = {{ effort = "medium" }}
allowed_params = {{ effort = ["medium", "high"] }}
recommendations = ["broad coding"]
[[providers.codex.models]]
id = "gpt-review"
restrictions = ["web_tools_disabled"]
[[providers.codex.bindings]]
label = "private-codex-label"
account = "acct-codex"
[providers.glm]
harness = "claude-code"
connection = {{ kind = "custom", endpoint = "https://api.example.com/api", protocol = "messages" }}
auth_family = "anthropic"
limits_source = "exec"
collector = {{ command = "/bin/true" }}
[[providers.glm.models]]
id = "glm-5.3"
[[providers.glm.bindings]]
label = "work"
account = "acct-glm"
"#,
            root = root.display()
        ),
    )
    .unwrap();
}

/// A disposable v2 home with two roles, two accounts, and a historical local
/// sample that public Quota-backed reads must not treat as current evidence.
fn home() -> tempfile::TempDir {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path();
    agent_run_store::fixture::initialize(root).unwrap();
    fs::create_dir_all(root.join("profiles")).unwrap();
    fs::write(root.join("profiles/review.md"),
        "+++\nrevision = \"r1\"\nwrite = false\nnetwork = false\nallow_external_read_roots = false\nskills = []\nmcp = []\nrequired_constraints = []\n+++\nReview.\n").unwrap();
    fs::write(root.join("profiles/code.md"),
        "+++\nrevision = \"c1\"\nwrite = true\nnetwork = false\nallow_external_read_roots = false\nskills = []\nmcp = []\nrequired_constraints = []\n+++\nCode.\n").unwrap();
    write_config(root, "native subscription");
    let mut store = agent_run_store::fixture::open(root).unwrap();
    for (id, family, reference) in [
        ("acct-codex", "openai", "named:codex:secret-codex"),
        ("acct-glm", "anthropic", "keychain:secret-glm:ref"),
    ] {
        store
            .register_account(&AccountRecord {
                account_id: id.parse().unwrap(),
                auth_family: family.parse().unwrap(),
                secret_ref: reference.parse().unwrap(),
                status: AccountStatus::Enabled,
            })
            .unwrap();
    }
    let at = agent_run_core::domain::now();
    store
        .conn
        .execute(
            "INSERT INTO capacity_samples(runtime,lane,window,target,source,remaining_percent,reset_at,observed_at,valid_until,payload_json,account_id,quota_key) \
             VALUES('glm','glm-5.3','5h',NULL,'collector',60.0,?1,?2,?3,'null','acct-glm','acct-glm::glm-5.3')",
            rusqlite::params![at + 3600.0, at - 10.0, at + 600.0],
        )
        .unwrap();
    temp
}

/// Counts every durable row a read must never touch.
fn footprint(root: &Path) -> Vec<i64> {
    let store = agent_run_store::fixture::open(root).unwrap();
    [
        "agents",
        "attempts",
        "attempt_quota_keys",
        "capacity_samples",
    ]
    .iter()
    .map(|table| {
        store
            .conn
            .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                row.get(0)
            })
            .unwrap()
    })
    .chain([store.quota_capacity_revision().unwrap()])
    .collect()
}

const QUOTA_AT: f64 = 10001.0;
const QUOTA_KNOWN: &str = include_str!("fixtures/quota-client-v1/snapshot-v2.json");
const QUOTA_UNKNOWN: &str = include_str!("fixtures/quota-client-v1/unknown-snapshot-v2.json");
const QUOTA_EXHAUSTED: &str = include_str!("fixtures/quota-client-v1/exhausted-snapshot-v2.json");

/// Builds an account from the checked-in v2 service fixture with explicit source scores.
fn quota_account(id: &str, provider: &str, offerings: &[(&str, &str, f64)]) -> Value {
    let fixture: Value = serde_json::from_str(&QUOTA_KNOWN.replace("acct-test", id)).unwrap();
    let mut account = fixture["result"]["accounts"][0].clone();
    let model = account["models"][0].clone();
    let window = account["windows"][0].clone();
    let mut models = Vec::new();
    let mut windows = Vec::new();
    for &(name, native, score) in offerings {
        let pool = format!("{id}::{name}");
        let mut m = model.clone();
        m["provider"] = json!(provider);
        m["model"] = json!(name);
        m["native_model"] = json!(native);
        m["quota_score"] = json!(score);
        m["limiting_window"]["pool"] = json!(pool);
        m["pools"] = json!([pool]);
        m["state_validity"]["at"] = json!(QUOTA_AT + 600.0);
        m["newest_observed_at"] = json!(QUOTA_AT - 10.0);
        m["reset_at"] = json!(QUOTA_AT + 7200.0);
        models.push(m);
        let mut w = window.clone();
        w["runtime"] = json!(provider);
        w["lane"] = json!(name);
        w["pool"] = json!(pool);
        w["models"] = json!([native]);
        w["observed_at"] = json!(QUOTA_AT - 10.0);
        w["valid_until"] = json!(QUOTA_AT + 600.0);
        w["last_reset_at"] = json!(QUOTA_AT + 7200.0);
        w["forecast"]["observed_at"] = json!(QUOTA_AT - 10.0);
        w["forecast"]["reset_at"] = json!(QUOTA_AT + 7200.0);
        w["evaluation"]["reset_at"] = json!(QUOTA_AT + 7200.0);
        windows.push(w);
    }
    account["models"] = json!(models);
    account["windows"] = json!(windows);
    account
}

/// Source facts are synthetic and independent of the old Run quota tables.
fn quota_snapshot(stale_and_exhausted: bool) -> agent_run_core::quota_client::Snapshot {
    let mut source: Value = serde_json::from_str(QUOTA_UNKNOWN).unwrap();
    let mut codex = source["result"]["accounts"][0].clone();
    codex["account"] = json!("acct-codex");
    let mut main = codex["models"][0].clone();
    main["model"] = json!("gpt-main");
    main["native_model"] = json!("gpt-native");
    let mut review = main.clone();
    review["model"] = json!("gpt-review");
    review["native_model"] = json!("gpt-review");
    codex["models"] = json!([main, review]);
    let glm = quota_account("acct-glm", "glm", &[("glm-5.3", "glm-5.3", 1.2)]);
    let mut a = quota_account(
        "acct-a",
        "a",
        &[("a-locked", "a-locked", 1.8), ("a-open", "a-open", 0.2)],
    );
    let mut b = quota_account("acct-b", "b", &[("b-open", "b-open", 1.2)]);
    if stale_and_exhausted {
        let exhausted: Value = serde_json::from_str(QUOTA_EXHAUSTED).unwrap();
        let mut model = exhausted["result"]["accounts"][0]["models"][0].clone();
        model["provider"] = json!("a");
        model["model"] = json!("a-locked");
        model["native_model"] = json!("a-locked");
        model["limiting_window"]["pool"] = json!("acct-a::a-locked");
        model["pools"] = json!(["acct-a::a-locked"]);
        model["state_validity"]["at"] = json!(QUOTA_AT + 7200.0);
        model["reset_at"] = json!(QUOTA_AT + 7200.0);
        model["exhausted_until"] = json!(QUOTA_AT + 7200.0);
        model["newest_observed_at"] = json!(QUOTA_AT - 10.0);
        a["models"][0] = model;
        b["models"][0]["state_validity"]["at"] = json!(QUOTA_AT);
    }
    source["result"]["evaluated_at"] = json!(QUOTA_AT);
    source["result"]["accounts"] = json!([codex, glm, a, b]);
    serde_json::from_value(source["result"].clone()).unwrap()
}

fn synthetic_models(
    root: &Path,
    query: &ModelsQuery,
    source: &agent_run_core::quota_client::Snapshot,
) -> Value {
    let (config, revision) = agent_run_config::provider_config::ProviderConfig::load(root).unwrap();
    let store = agent_run_store::fixture::open(root).unwrap();
    agent_run_core::capacity::quota_projection::models_at(
        &store,
        root,
        &config,
        &revision,
        query,
        Some(source),
        QUOTA_AT,
    )
    .unwrap()
}

fn synthetic_order(
    root: &Path,
    query: &CapacityOrderQuery,
    source: &agent_run_core::quota_client::Snapshot,
) -> Value {
    let (config, revision) = agent_run_config::provider_config::ProviderConfig::load(root).unwrap();
    let store = agent_run_store::fixture::open(root).unwrap();
    agent_run_core::capacity::quota_projection::order_at(
        &store,
        &config,
        &revision,
        query,
        Some(source),
        QUOTA_AT,
    )
    .unwrap()
}

fn synthetic_limits(
    root: &Path,
    source: &agent_run_core::quota_client::Snapshot,
    at: f64,
) -> Value {
    let (config, revision) = agent_run_config::provider_config::ProviderConfig::load(root).unwrap();
    let store = agent_run_store::fixture::open(root).unwrap();
    agent_run_core::capacity::quota_projection::limits_at(
        &store,
        &config,
        &revision,
        Some(source),
        at,
    )
    .unwrap()
}

fn synthetic_guide(root: &Path, source: &agent_run_core::quota_client::Snapshot) -> Value {
    let (config, revision) = agent_run_config::provider_config::ProviderConfig::load(root).unwrap();
    let store = agent_run_store::fixture::open(root).unwrap();
    agent_run_core::capacity::quota_projection::delegation_guide_at(
        &store,
        root,
        &config,
        &revision,
        &ModelsQuery::default(),
        Some(source),
        QUOTA_AT,
    )
    .unwrap()
}

/// Records routed handoffs without starting a model or claiming a successful run.
struct RoutedFixture {
    /// Route returned for the provider's current root-approved configuration.
    route: std::sync::Mutex<agent_run_core::system_launch::SystemRoute>,
    /// Handoffs observed after admission.
    launches: std::sync::Arc<std::sync::Mutex<Vec<agent_run_core::system_launch::SystemRoute>>>,
}

impl agent_run_core::system_launch::SystemLauncher for RoutedFixture {
    fn route(
        &self,
        _provider: &agent_run_domain::catalog::ProviderId,
        _harness: agent_run_domain::catalog::HarnessId,
    ) -> agent_run_domain::Result<agent_run_core::system_launch::SystemRoute> {
        Ok(self.route.lock().unwrap().clone())
    }

    fn preflight_resume(
        &self,
        _parent: agent_run_domain::domain::AgentId,
        _route: agent_run_core::system_launch::SystemRoute,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = agent_run_domain::Result<String>> + Send + 'static>,
    > {
        Box::pin(async { Err(agent_run_domain::Error::ExecutionChanged) })
    }

    fn launch(
        &self,
        _home: std::path::PathBuf,
        _execution: agent_run_domain::domain::AgentId,
        route: agent_run_core::system_launch::SystemRoute,
        _lane: agent_run_core::broker_writer::BrokerLane,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = agent_run_domain::Result<()>> + Send + 'static>,
    > {
        let launches = self.launches.clone();
        Box::pin(async move {
            launches.lock().unwrap().push(route);
            Err(agent_run_domain::Error::Runtime(
                "fixture stops before spawning".into(),
            ))
        })
    }
}

/// A created execution freezes its route; replay and a changed launcher never start it twice.
#[tokio::test]
async fn system_route_is_frozen_and_handoff_never_falls_back() {
    use agent_run_core::system_launch::SystemRoute;
    use agent_run_domain::catalog::HarnessId;
    let temp = home();
    let root = temp.path().canonicalize().unwrap();
    // This test never starts the binary; use its own verified executable
    // instead of assuming a platform-specific path to `true`.
    let config_path = root.join("config.toml");
    let original = fs::read_to_string(&config_path).unwrap();
    let executable = toml::Value::String(
        std::env::current_exe()
            .unwrap()
            .to_string_lossy()
            .into_owned(),
    );
    let portable = original.replace("\"/bin/true\"", &executable.to_string());
    assert_ne!(portable, original);
    fs::write(&config_path, portable).unwrap();
    let writer = common::writer::Harness::start(&root);
    let launches = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let route = SystemRoute {
        provider: "glm".parse().unwrap(),
        harness: HarnessId::ClaudeCode,
        state_home: root.clone(),
        private_home: root.clone(),
        operator_socket: root.join("operator.sock"),
        writer_socket: root.join("writer.sock"),
        worker_socket: root.join("worker.sock"),
        fingerprint: "a".repeat(64).parse().unwrap(),
    };
    let launcher = std::sync::Arc::new(RoutedFixture {
        route: std::sync::Mutex::new(route.clone()),
        launches: launches.clone(),
    });
    let service = Service::new(root.clone())
        .with_writer(writer.broker())
        .with_system_launcher(launcher.clone());
    let request: agent_run_domain::ProviderStartRequest = serde_json::from_value(json!({
        "provider":"glm", "model":"glm-5.3", "profile":"review", "task":"fixture",
        "workdir":root, "request_id":"system-route-fixture"
    }))
    .unwrap();
    let before = footprint(&root);
    launcher.route.lock().unwrap().harness = HarnessId::Codex;
    assert!(service.admit_provider(request.clone()).await.is_err());
    assert_eq!(footprint(&root), before);
    *launcher.route.lock().unwrap() = route.clone();
    let first = service.start_provider(request.clone()).await.unwrap();
    assert_eq!(first["created"], true);
    assert_eq!(
        launches.lock().unwrap().as_slice(),
        std::slice::from_ref(&route)
    );
    let id = first["agent_id"].as_str().unwrap().parse().unwrap();
    let store = agent_run_store::writer::StoreReader::open(&root).unwrap();
    let row = store.get(&id).unwrap();
    let frozen = agent_run_core::service::ProviderLaunchIdentity::read(&row).unwrap();
    assert_eq!(frozen.system_route, Some(route));
    drop(store);
    let replay = service.start_provider(request).await.unwrap();
    assert_eq!(replay["created"], false);
    assert_eq!(launches.lock().unwrap().len(), 1);

    {
        let mut changed = launcher.route.lock().unwrap();
        changed.fingerprint = "b".repeat(64).parse().unwrap();
    }
    let error = service
        .admit_provider_resume(&row, "next".into(), None, None, None)
        .await
        .unwrap_err();
    assert!(matches!(error, agent_run_domain::Error::ExecutionChanged));
    let local = Service::new(root).with_writer(writer.broker());
    let error = local
        .admit_provider_resume(&row, "next".into(), None, None, None)
        .await
        .unwrap_err();
    assert!(matches!(error, agent_run_domain::Error::Unsupported(_)));
    assert_eq!(launches.lock().unwrap().len(), 1);
}

/// Assertions only test the frozen intersection: missing/capped/restricted
/// grants have no durable effects; exact replay survives deleted directories.
#[tokio::test]
async fn required_mcp_checks_frozen_grants_before_effects_and_replays_without_workdir() {
    let temp = home();
    let root = temp.path().canonicalize().unwrap();
    let config_path = root.join("config.toml");
    let mut config = fs::read_to_string(&config_path)
        .unwrap()
        .replace("/bin/true", "/usr/bin/true");
    config.push_str("\n[mcp.pool]\ntransport='stdio'\ncommand='/usr/bin/true'\nglobal=true\nallowed_tools=['join','read','write']\n[mcp.empty]\ntransport='stdio'\ncommand='/usr/bin/true'\nglobal=true\nallowed_tools=[]\n");
    fs::write(&config_path, config).unwrap();
    fs::write(root.join("profiles/code.md"), "+++\nrevision='mcp-test'\nwrite=true\nnetwork=false\nallow_external_read_roots=false\nskills=[]\nmcp=[{name='pool',allowed_tools=['join','read']}]\nrequired_constraints=[]\n+++\nCode.\n").unwrap();
    fs::write(root.join("profiles/research.md"), "+++\nrevision='research-test'\nwrite=false\nnetwork=true\nallow_external_read_roots=false\nskills=[]\nmcp=[]\nrequired_constraints=['research_tools_only']\n+++\nResearch.\n").unwrap();
    let writer = common::writer::Harness::start(&root);
    let service = Service::new(root.clone()).with_writer(writer.broker());
    let workdir = root.join("work");
    fs::create_dir(&workdir).unwrap();
    let request = |id: &str, required: Value| -> agent_run_domain::ProviderStartRequest {
        serde_json::from_value(json!({"provider":"glm","model":"glm-5.3","profile":"code","task":"task","workdir":workdir,"request_id":id,"required_mcp":required})).unwrap()
    };
    let before = footprint(&root);
    for required in [
        json!({"absent":[]}),
        json!({"pool":["write"]}),
        json!({"empty":["join"]}),
    ] {
        let error = service
            .admit_provider(request("refused", required))
            .await
            .unwrap_err();
        assert!(error.to_string().contains("required MCP"), "{error}");
        assert_eq!(footprint(&root), before);
    }
    let mut restricted = request("restricted", json!({"pool":[]}));
    restricted.provider = "codex".parse().unwrap();
    restricted.model = "gpt-main".into();
    restricted.profile = "research".into();
    assert!(service.admit_provider(restricted).await.is_err());
    assert_eq!(footprint(&root), before);
    let first = service
        .admit_provider(request(
            "durable",
            json!({"pool":["read","join"],"empty":[]}),
        ))
        .await
        .unwrap();
    assert_eq!(first["created"], true);
    let admitted = footprint(&root);
    fs::remove_dir(&workdir).unwrap();
    let replay = service
        .admit_provider(request(
            "durable",
            json!({"empty":[],"pool":["join","read"]}),
        ))
        .await
        .unwrap();
    assert_eq!(replay["created"], false);
    assert_eq!(replay["agent_id"], first["agent_id"]);
    assert_eq!(footprint(&root), admitted);
    assert!(matches!(
        service
            .admit_provider(request("durable", json!({"pool":["join"]})))
            .await,
        Err(agent_run_domain::Error::Conflict)
    ));
    assert!(
        service
            .admit_provider(request("new-missing", json!({})))
            .await
            .is_err()
    );
    assert_eq!(footprint(&root), admitted);
}

/// The unfiltered catalog is one revisioned, account-free snapshot in
/// provider order with explicit offerings, grants, and cached standing.
#[tokio::test]
async fn catalog_is_revisioned_ordered_and_account_free() {
    let temp = home();
    let root = temp.path();
    let before = footprint(root);
    let service = Service::new(root.to_path_buf());
    let catalog = service.models(ModelsQuery::default()).await.unwrap();
    let bytes = fs::read(root.join("config.toml")).unwrap();
    assert_eq!(catalog["schema_version"], 2);
    assert_eq!(
        catalog["config_revision"],
        agent_run_platform::fs::sha256(&bytes)
    );
    assert_eq!(catalog["capacity_revision"], before[4]);
    let providers = catalog["providers"].as_array().unwrap();
    assert_eq!(
        providers[0]["provider"], "codex",
        "unavailable Quota leaves both providers unknown"
    );
    assert_eq!(providers[1]["models"][0]["quota"]["status"], "unknown");
    assert_eq!(catalog["quota_coverage"]["incomplete"], true);
    let codex = &providers[0];
    assert_eq!(codex["recommendations"], json!(["native subscription"]));
    assert_eq!(codex["connection"], json!({"kind": "native"}));
    let main = &codex["models"][0];
    assert_eq!(main["native_model"], "gpt-native");
    assert_eq!(main["allowed_params"]["effort"], json!(["medium", "high"]));
    assert_eq!(main["params"]["effort"], "medium");
    assert_eq!(main["quota"]["status"], "unknown");
    assert_eq!(main["profiles"], json!(["code", "review"]));
    assert_eq!(
        codex["models"][1]["restrictions"],
        json!(["web_tools_disabled"])
    );
    let roles: Vec<&str> = catalog["profiles"]
        .as_array()
        .unwrap()
        .iter()
        .map(|role| role["name"].as_str().unwrap())
        .collect();
    assert_eq!(roles, ["code", "review"]);
    let text = catalog.to_string();
    for private in [
        "acct-",
        "private-codex-label",
        "secret-",
        "keychain",
        "api.example.com",
    ] {
        assert!(!text.contains(private), "{private} leaked: {text}");
    }
    let order = service
        .capacity_order(CapacityOrderQuery::default())
        .await
        .unwrap();
    assert_eq!(order["capacity_revision"], before[4]);
    assert!(!order.to_string().contains("acct-"));
    assert_eq!(footprint(root), before, "reads never write");
}

/// Catalog and actual admission share the same native role compatibility:
/// ordinary read-only/network roles remain unsupported, while the explicit
/// research constraint permits one admitted run with native controlled tools.
#[tokio::test]
async fn research_catalog_matches_actual_static_admission() {
    let temp = home();
    let root = temp.path().canonicalize().unwrap();
    let writer = common::writer::Harness::start(&root);
    let service = Service::new(root.clone()).with_writer(writer.broker());
    let body = |constraints: &str| {
        format!(
            "+++\nrevision=\"research-test\"\nwrite=false\nnetwork=true\nallow_external_read_roots=false\nskills=[]\nmcp=[]\nrequired_constraints={constraints}\n+++\nSearch and save reports.\n"
        )
    };
    fs::write(root.join("profiles/research.md"), body("[]")).unwrap();
    let query = ModelsQuery {
        provider: Some("codex".into()),
        model: Some("gpt-main".into()),
        profile: Some("research".into()),
    };
    let old = service.models(query.clone()).await.unwrap();
    assert!(old["providers"].as_array().unwrap().is_empty());
    let request = || {
        serde_json::from_value(json!({"provider":"codex","model":"gpt-main","profile":"research","task":"Search with sources","workdir":root})).unwrap()
    };
    assert!(service.admit_provider(request()).await.is_err());
    fs::write(
        root.join("profiles/research.md"),
        body("[\"research_tools_only\"]"),
    )
    .unwrap();
    let current = service.models(query).await.unwrap();
    assert_eq!(current["providers"].as_array().unwrap().len(), 1);
    // The catalog fixture's inert executable must exist for actual admission.
    let config = fs::read_to_string(root.join("config.toml"))
        .unwrap()
        .replace("/bin/true", "/usr/bin/true");
    fs::write(root.join("config.toml"), config).unwrap();
    let before = footprint(&root);
    service.admit_provider(request()).await.unwrap();
    assert_eq!(footprint(&root)[0], before[0] + 1, "research is admitted");
}

/// Exact filters narrow the same snapshot; unknown values and fields are
/// typed validation errors on every transport entry.
#[tokio::test]
async fn filters_are_exact_and_typed() {
    let temp = home();
    let root = temp.path();
    let service = Service::new(root.to_path_buf());
    let filtered = service
        .models(ModelsQuery {
            provider: Some("codex".into()),
            profile: Some("review".into()),
            model: Some("gpt-main".into()),
        })
        .await
        .unwrap();
    let providers = filtered["providers"].as_array().unwrap();
    assert_eq!(providers.len(), 1);
    assert_eq!(providers[0]["models"].as_array().unwrap().len(), 1);
    assert_eq!(filtered["profiles"].as_array().unwrap().len(), 1);
    let order = service
        .capacity_order(CapacityOrderQuery {
            model: Some("glm-5.3".into()),
        })
        .await
        .unwrap();
    assert_eq!(order["providers"].as_array().unwrap().len(), 1);
    assert_eq!(order["providers"][0]["provider"], "glm");
    for bad in [
        json!({"provider": "missing"}),
        json!({"model": "gpt"}),
        json!({"profile": "missing"}),
        json!({"unknown": 1}),
    ] {
        let error = dispatch::call(&service, "models", bad.clone())
            .await
            .unwrap_err();
        assert_eq!(error.machine_code().as_str(), "ValidationError", "{bad}");
    }
    let error = dispatch::call(&service, "capacity_order", json!({"model": "nope"}))
        .await
        .unwrap_err();
    assert_eq!(error.machine_code().as_str(), "ValidationError");
}

/// Editing configured recommendation prose yields a new revision and the new
/// text on the next read; a malformed edit is rejected, never half-applied.
#[tokio::test]
async fn recommendation_edits_are_reflected_by_revision() {
    let temp = home();
    let root = temp.path();
    let service = Service::new(root.to_path_buf());
    let first = service.models(ModelsQuery::default()).await.unwrap();
    write_config(root, "prefer for long refactors");
    let second = service.models(ModelsQuery::default()).await.unwrap();
    assert_ne!(first["config_revision"], second["config_revision"]);
    let codex = second["providers"]
        .as_array()
        .unwrap()
        .iter()
        .find(|provider| provider["provider"] == "codex")
        .unwrap();
    assert_eq!(
        codex["recommendations"],
        json!(["prefer for long refactors"])
    );
    // Harnesses without providers is neither a catalog nor the empty one.
    fs::write(
        root.join("config.toml"),
        "schema_version = 2\n[harnesses.codex]\nbinary = '/bin/true'\nhome = '/tmp'\n",
    )
    .unwrap();
    assert!(service.models(ModelsQuery::default()).await.is_err());
}

/// Schema-1 order and limits explicitly require migration to schema 2.
#[tokio::test]
async fn schema_one_filters_are_unsupported() {
    let temp = tempfile::tempdir().unwrap();
    agent_run_store::fixture::initialize(temp.path()).unwrap();
    fs::write(temp.path().join("config.toml"), "schema_version = 1\n").unwrap();
    let service = Service::new(temp.path().to_path_buf());
    let error = service
        .models(ModelsQuery {
            model: Some("x".into()),
            ..Default::default()
        })
        .await
        .unwrap_err();
    assert_eq!(error.machine_code().as_str(), "Unsupported");
    let error = service
        .capacity_order(CapacityOrderQuery {
            model: Some("x".into()),
        })
        .await
        .unwrap_err();
    assert_eq!(error.machine_code().as_str(), "Unsupported");
    let error = service
        .capacity_order(CapacityOrderQuery::default())
        .await
        .unwrap_err();
    assert_eq!(error.machine_code().as_str(), "Unsupported");
    assert!(error.to_string().contains("schema_version 2"));
    let error = service.limits().await.unwrap_err();
    assert_eq!(error.machine_code().as_str(), "Unsupported");
    assert!(error.to_string().contains("schema_version 2"));
}

/// Inserts one account-bound sample on `lane` with explicit freshness.
fn sample(root: &Path, provider: &str, account: &str, lane: &str, remaining: f64, valid: f64) {
    let at = agent_run_core::domain::now();
    agent_run_store::fixture::open(root)
        .unwrap()
        .conn
        .execute(
            "INSERT INTO capacity_samples(runtime,lane,window,target,source,remaining_percent,reset_at,observed_at,valid_until,payload_json,account_id,quota_key) \
             VALUES(?1,?2,'5h',NULL,'collector',?3,?4,?5,?6,'null',?7,?8)",
            rusqlite::params![
                provider,
                lane,
                remaining,
                at + 3600.0,
                at - 10.0,
                at + valid,
                account,
                format!("{account}::{lane}")
            ],
        )
        .unwrap();
}

/// A home where provider `a` offers a high-capacity model restricted to
/// roles without network (`web_tools_disabled`) plus a low-capacity open
/// model, and provider `b` offers one mid-capacity open model.
fn ranking_home() -> tempfile::TempDir {
    let temp = home();
    let root = temp.path();
    fs::write(root.join("profiles/research.md"),
        "+++\nrevision = \"n1\"\nwrite = false\nnetwork = true\nallow_external_read_roots = false\nskills = []\nmcp = []\nrequired_constraints = []\n+++\nResearch.\n").unwrap();
    fs::write(
        root.join("config.toml"),
        format!(
            r#"schema_version = 2
[harnesses.codex]
binary = "/bin/true"
home = "{root}/codex"
[harnesses.claude-code]
binary = "/bin/true"
home = "{root}/claude"
[providers.a]
harness = "claude-code"
connection = {{ kind = "custom", endpoint = "https://a.example.com/api", protocol = "messages" }}
auth_family = "anthropic"
limits_source = "none"
[[providers.a.models]]
id = "a-locked"
restrictions = ["web_tools_disabled"]
[[providers.a.models]]
id = "a-open"
[[providers.a.bindings]]
label = "main"
account = "acct-a"
[providers.b]
harness = "claude-code"
connection = {{ kind = "custom", endpoint = "https://b.example.com/api", protocol = "messages" }}
auth_family = "anthropic"
limits_source = "none"
[[providers.b.models]]
id = "b-open"
[[providers.b.bindings]]
label = "main"
account = "acct-b"
"#,
            root = root.display()
        ),
    )
    .unwrap();
    let mut store = agent_run_store::fixture::open(root).unwrap();
    for (id, reference) in [
        ("acct-a", "keychain:fake-a:ref"),
        ("acct-b", "keychain:fake-b:ref"),
    ] {
        store
            .register_account(&AccountRecord {
                account_id: id.parse().unwrap(),
                auth_family: "anthropic".parse().unwrap(),
                secret_ref: reference.parse().unwrap(),
                status: AccountStatus::Enabled,
            })
            .unwrap();
    }
    sample(root, "a", "acct-a", "a-locked", 90.0, 600.0);
    sample(root, "a", "acct-a", "a-open", 10.0, 600.0);
    sample(root, "b", "acct-b", "b-open", 60.0, 600.0);
    temp
}

/// Returns `(provider, score, model ids)` in catalog order.
fn standing(catalog: &Value) -> Vec<(String, f64, Vec<String>)> {
    catalog["providers"]
        .as_array()
        .unwrap()
        .iter()
        .map(|provider| {
            (
                provider["provider"].as_str().unwrap().to_owned(),
                provider["score"].as_f64().unwrap(),
                provider["models"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|model| model["model"].as_str().unwrap().to_owned())
                    .collect(),
            )
        })
        .collect()
}

/// A profile filter ranks providers by the offerings it retains: an
/// offering the role cannot use never lends its score or position.
#[tokio::test]
async fn profile_filter_ranks_only_retained_offerings() {
    let temp = ranking_home();
    let root = temp.path();
    let source = quota_snapshot(false);
    let all = standing(&synthetic_models(root, &ModelsQuery::default(), &source));
    assert_eq!(all[0].0, "a", "a-locked has the most capacity: {all:?}");
    let research = synthetic_models(
        root,
        &ModelsQuery {
            profile: Some("research".into()),
            ..Default::default()
        },
        &source,
    );
    let filtered = standing(&research);
    assert_eq!(filtered[0].0, "b", "{filtered:?}");
    assert_eq!(filtered[1].0, "a");
    assert_eq!(filtered[1].2, ["a-open"]);
    let a_open = research["providers"][1]["models"][0]["quota"]["best_priority"]
        .as_f64()
        .unwrap();
    assert_eq!(filtered[1].1, a_open, "score comes from a-open alone");
    assert!(filtered[0].1 > filtered[1].1);
    assert!(filtered[1].1 < all[0].1);
}

/// Standing states its evidence: a current sample is `fresh`, an expired
/// one `stale` with its age, a never-observed lane `missing`, and an
/// active exhaustion fact reports its reset — never an account identity.
#[tokio::test]
async fn quota_standing_reports_freshness_and_exhaustion() {
    let temp = ranking_home();
    let root = temp.path();
    let source = quota_snapshot(true);
    let reset = QUOTA_AT + 7200.0;
    let service = Service::new(root.to_path_buf());
    let order = synthetic_order(root, &CapacityOrderQuery::default(), &source);
    let quota = |provider: &str, model: &str| {
        order["providers"]
            .as_array()
            .unwrap()
            .iter()
            .find(|entry| entry["provider"] == provider)
            .unwrap()["models"]
            .as_array()
            .unwrap()
            .iter()
            .find(|entry| entry["model"] == model)
            .unwrap()["quota"]
            .clone()
    };
    let locked = quota("a", "a-locked");
    assert_eq!(locked["status"], "exhausted");
    assert_eq!(locked["evidence"], "fresh");
    assert_eq!(locked["exhausted_until"], reset);
    let stale = quota("b", "b-open");
    assert_eq!(stale["status"], "unknown");
    assert_eq!(stale["evidence"], "stale");
    assert!(stale["newest_observed_at"].as_f64().is_some());
    assert_eq!(quota("a", "a-open")["evidence"], "fresh");
    assert!(order["ranked_at"].as_f64().is_some());
    let catalog = service.models(ModelsQuery::default()).await.unwrap();
    let codexless = catalog.to_string();
    assert!(!codexless.contains("acct-"));
    let missing = home();
    let missing_catalog = synthetic_models(
        missing.path(),
        &ModelsQuery::default(),
        &quota_snapshot(false),
    );
    assert_eq!(
        missing_catalog["providers"][1]["models"][0]["quota"]["evidence"],
        "missing"
    );
}

/// The delegation guide renders one committed snapshot as compact text:
/// capacity order, configured guidance at both levels, per-model standing
/// with derived freshness, admissible profiles, params, restrictions — and
/// no account, credential, or endpoint identity.
#[tokio::test]
async fn delegation_guide_renders_guidance_order_and_privacy() {
    let temp = home();
    let root = temp.path();
    let guide = synthetic_guide(root, &quota_snapshot(false));
    let text = guide.as_str().expect("guide is a string result");
    let glm = text.find("provider glm").expect("glm listed");
    let codex = text.find("provider codex").expect("codex listed");
    assert!(glm < codex, "capacity order: glm first\n{text}");
    assert!(text.contains("provider glm (harness claude-code)"));
    // Configured recommendation prose at both levels, and only where
    // configured: glm carries no guidance, so nothing is fabricated.
    assert!(text.contains("provider guidance: native subscription"));
    assert!(text.contains("model guidance: broad coding"));
    assert_eq!(text.matches("provider guidance:").count(), 1);
    assert_eq!(text.matches("model guidance:").count(), 1);
    // One short line per exact model id, with honest standing: the fresh
    // glm sample reads available, the never-observed codex lanes do not.
    assert!(text.contains("- glm-5.3: quota available, evidence fresh"));
    assert!(text.contains("- gpt-main: quota unknown, evidence missing"));
    assert!(text.contains("- gpt-review: quota unknown, evidence missing"));
    // Identical admissible profiles are stated once per provider, not
    // repeated per model.
    assert!(
        text.contains("provider codex (harness codex) — all models admit: code, review"),
        "{text}"
    );
    assert_eq!(text.matches("profiles:").count(), 0, "{text}");
    assert!(text.contains("params: effort=medium; allowed effort: medium|high"));
    assert!(text.contains("restrictions: web_tools_disabled"));
    for private in [
        "acct-",
        "private-codex-label",
        "secret-",
        "keychain",
        "api.example.com",
        "roles_sha",
        "config_revision",
    ] {
        assert!(!text.contains(private), "{private} leaked: {text}");
    }
}

/// Guide standing stays honest under staleness and exhaustion: an expired
/// sample reads `stale` with its derived age, an active latch reads
/// `exhausted` with a derived reset horizon, and none looks healthy.
#[tokio::test]
async fn delegation_guide_reports_stale_and_exhausted_standings() {
    let temp = ranking_home();
    let root = temp.path();
    let text = synthetic_guide(root, &quota_snapshot(true))
        .as_str()
        .unwrap()
        .to_owned();
    assert!(
        text.contains("- a-locked: quota exhausted, evidence fresh, resets in 2h"),
        "{text}"
    );
    assert!(
        text.contains("- b-open: quota unknown, evidence stale, sample age"),
        "{text}"
    );
    assert!(
        text.contains("- a-open: quota available, evidence fresh"),
        "{text}"
    );
}

/// The guide is schema-2 only (legacy configs are typed `Unsupported`),
/// takes no arguments, and states an explicitly empty catalog as such.
#[tokio::test]
async fn delegation_guide_is_strict_schema2_and_states_empty_catalogs() {
    let legacy = tempfile::tempdir().unwrap();
    agent_run_store::fixture::initialize(legacy.path()).unwrap();
    fs::write(legacy.path().join("config.toml"), "schema_version = 1\n").unwrap();
    let service = Service::new(legacy.path().to_path_buf());
    assert_eq!(
        service
            .delegation_guide()
            .await
            .unwrap_err()
            .machine_code()
            .as_str(),
        "Unsupported"
    );
    for bad in [json!({"unexpected": 1}), json!([])] {
        let error = dispatch::call(&service, "delegation_guide", bad.clone())
            .await
            .unwrap_err();
        assert_eq!(error.machine_code().as_str(), "ValidationError", "{bad}");
    }
    let empty = tempfile::tempdir().unwrap();
    agent_run_store::fixture::initialize(empty.path()).unwrap();
    fs::write(empty.path().join("config.toml"), "schema_version = 2\n").unwrap();
    let text = Service::new(empty.path().to_path_buf())
        .delegation_guide()
        .await
        .unwrap()
        .as_str()
        .unwrap()
        .to_owned();
    assert!(
        text.contains("No providers are currently configured."),
        "{text}"
    );
}

/// Consolidated reads reuse exact catalog filters, typed errors and stored
/// window facts while diagnostic ranking preserves numerical/account-free standing.
#[tokio::test]
async fn consolidated_guide_filters_and_limits_scores_are_read_only() {
    let temp = home();
    let root = temp.path();
    let service = Service::new(root.to_path_buf());
    let before = footprint(root);
    let text = dispatch::call(
        &service,
        "delegation_guide",
        json!({"provider":"codex","model":"gpt-main","profile":"review"}),
    )
    .await
    .unwrap()
    .as_str()
    .unwrap()
    .to_owned();
    assert!(text.contains("provider codex"));
    assert!(text.contains("gpt-main"));
    assert!(text.contains("review"));
    assert!(text.contains("effort=medium; allowed effort: medium|high"));
    assert!(!text.contains("gpt-review"));
    assert!(!text.contains("provider glm"));
    for bad in [
        json!({"provider":"missing"}),
        json!({"model":"gpt"}),
        json!({"profile":"missing"}),
        json!({"unknown":1}),
    ] {
        let error = dispatch::call(&service, "delegation_guide", bad)
            .await
            .unwrap_err();
        assert_eq!(error.machine_code().as_str(), "ValidationError");
    }
    let limits = service.limits().await.unwrap();
    assert_eq!(limits["observed_at"], limits["ranking"]["ranked_at"]);
    assert_eq!(limits["ranking"]["schema_version"], 2);
    assert!(limits["items"].as_array().unwrap().is_empty());
    assert_eq!(limits["coverage"]["state"], "unknown");
    assert_eq!(limits["coverage"]["incomplete"], true);
    assert!(
        limits["ranking"]["providers"]
            .as_array()
            .unwrap()
            .iter()
            .all(|provider| provider["score"].is_null())
    );
    assert!(!limits["ranking"].to_string().contains("acct-"));
    assert_eq!(before, footprint(root));
}

/// Quota windows and local eligibility are projected from one local read snapshot.
#[test]
fn diagnostic_windows_and_ranking_share_one_committed_snapshot() {
    let temp = home();
    let root = temp.path();
    let (config, revision) = agent_run_config::provider_config::ProviderConfig::load(root).unwrap();
    let store = agent_run_store::fixture::open(root).unwrap();
    let source = quota_snapshot(false);
    let read = store.conn.unchecked_transaction().unwrap();
    let before = agent_run_core::capacity::quota_projection::limits_at(
        &store,
        &config,
        &revision,
        Some(&source),
        QUOTA_AT,
    )
    .unwrap();
    agent_run_store::fixture::open(root)
        .unwrap()
        .disable_account(&"acct-glm".parse().unwrap())
        .unwrap();
    let same = agent_run_core::capacity::quota_projection::limits_at(
        &store,
        &config,
        &revision,
        Some(&source),
        QUOTA_AT,
    )
    .unwrap();
    assert_eq!(
        same["ranking"]["capacity_revision"],
        before["ranking"]["capacity_revision"]
    );
    assert_eq!(same["items"], before["items"]);
    assert_eq!(same["ranking"], before["ranking"]);
    read.commit().unwrap();
    let after = agent_run_core::capacity::quota_projection::limits_at(
        &store,
        &config,
        &revision,
        Some(&source),
        QUOTA_AT,
    )
    .unwrap();
    assert_eq!(after["items"], before["items"]);
    let glm = after["ranking"]["providers"]
        .as_array()
        .unwrap()
        .iter()
        .find(|provider| provider["provider"] == "glm")
        .unwrap();
    assert_eq!(glm["models"][0]["quota"]["status"], "no_eligible_account");
    assert_ne!(
        after["ranking"]["capacity_revision"],
        before["ranking"]["capacity_revision"]
    );
}

/// Two raw windows survive even when the source class is exhausted, and
/// stale windows retain reset facts without inventing remaining quota.
#[test]
fn diagnostic_standing_uses_all_windows_and_keeps_stale_facts() {
    use agent_run_core::quota_client::{ModelState, StateValidity};
    let temp = home();
    let root = temp.path();
    let mut source = quota_snapshot(false);
    let account = source
        .accounts
        .iter_mut()
        .find(|a| a.account == "acct-glm")
        .unwrap();
    account.windows[0].forecast.remaining_percent = Some(100.0);
    let mut weekly =
        quota_account("acct-glm", "glm", &[("glm-5.3", "glm-5.3", 1.2)])["windows"][0].clone();
    weekly["window"] = json!("weekly");
    weekly["forecast"]["remaining_percent"] = json!(0.0);
    account
        .windows
        .push(serde_json::from_value(weekly).unwrap());
    account.models[0].state = ModelState::Exhausted;
    account.models[0].state_validity = StateValidity::Until {
        at: QUOTA_AT + 600.0,
    };
    account.models[0].quota_score = None;
    account.models[0].exhausted_until = Some(QUOTA_AT + 7200.0);
    let limits = synthetic_limits(root, &source, QUOTA_AT);
    let items = limits["items"].as_array().unwrap();
    assert!(items.iter().any(|item| item["remaining_percent"] == 100.0));
    assert!(items.iter().any(|item| item["remaining_percent"] == 0.0));
    let glm = limits["ranking"]["providers"]
        .as_array()
        .unwrap()
        .iter()
        .find(|provider| provider["provider"] == "glm")
        .unwrap();
    assert_eq!(glm["models"][0]["quota"]["status"], "exhausted");
    let stale = synthetic_limits(root, &source, QUOTA_AT + 601.0);
    for item in stale["items"].as_array().unwrap() {
        assert_eq!(item["known"], false);
        assert!(item["remaining_percent"].is_null());
        assert!(item["reset_at"].is_number());
    }
    let glm = stale["ranking"]["providers"]
        .as_array()
        .unwrap()
        .iter()
        .find(|provider| provider["provider"] == "glm")
        .unwrap();
    assert_eq!(glm["models"][0]["quota"]["status"], "unknown");
    assert_eq!(glm["models"][0]["quota"]["evidence"], "stale");
}

/// Operator `limits` keeps two accounts sharing one provider lane apart.
#[test]
fn limits_keep_account_bound_rows_distinct() {
    let temp = ranking_home();
    let root = temp.path();
    let mut source = quota_snapshot(false);
    let extra = quota_account("acct-b", "a", &[("a-open", "a-open", 1.0)])["windows"][0].clone();
    source
        .accounts
        .iter_mut()
        .find(|a| a.account == "acct-b")
        .unwrap()
        .windows
        .push(serde_json::from_value(extra).unwrap());
    let limits = synthetic_limits(root, &source, QUOTA_AT);
    let pools: Vec<(String, String)> = limits["items"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|item| item["key"]["lane"] == "a-open")
        .map(|item| {
            (
                item["account"].as_str().unwrap().to_owned(),
                item["pool"].as_str().unwrap().to_owned(),
            )
        })
        .collect();
    assert_eq!(
        pools,
        [
            ("acct-a".to_owned(), "acct-a::a-open".to_owned()),
            ("acct-b".to_owned(), "acct-b::a-open".to_owned())
        ]
    );
    assert!(!limits.to_string().contains("keychain"));
}

/// A provider score is the best priority times an unrestricted positive
/// finite multiplier; when that product overflows it saturates at
/// `f64::MAX`, so the public order stays finite JSON (never `null`, NaN or
/// infinity). Saturated providers tie and fall back to the deterministic
/// status/provider-id order; a saturated score still outranks a finite one.
#[tokio::test]
async fn overflowing_provider_scores_saturate_in_the_public_order() {
    let temp = ranking_home();
    let root = temp.path();
    let path = root.join("config.toml");
    let config = fs::read_to_string(&path).unwrap();
    let huge = config.replace(
        "limits_source = \"none\"\n",
        "limits_source = \"none\"\npriority_multiplier = 1.7976931348623157e308\n",
    );
    assert_ne!(huge, config);
    fs::write(&path, &huge).unwrap();
    let scores = |order: &serde_json::Value| -> Vec<(String, serde_json::Value)> {
        order["providers"]
            .as_array()
            .unwrap()
            .iter()
            .map(|entry| {
                (
                    entry["provider"].as_str().unwrap().to_owned(),
                    entry["score"].clone(),
                )
            })
            .collect()
    };
    let order = synthetic_order(root, &CapacityOrderQuery::default(), &quota_snapshot(false));
    let wire: serde_json::Value = serde_json::from_str(&order.to_string()).unwrap();
    assert_eq!(
        scores(&wire),
        vec![
            ("a".to_owned(), serde_json::json!(f64::MAX)),
            ("b".to_owned(), serde_json::json!(f64::MAX))
        ],
        "{wire}"
    );
    assert_eq!(
        scores(&synthetic_order(
            root,
            &CapacityOrderQuery::default(),
            &quota_snapshot(false)
        )),
        scores(&wire),
        "deterministic"
    );
    // Only `b` overflows: its saturated score outranks `a`'s finite one.
    let mixed = config.replacen(
        "[providers.b]\nharness = \"claude-code\"",
        "[providers.b]\npriority_multiplier = 1.7976931348623157e308\nharness = \"claude-code\"",
        1,
    );
    assert_ne!(mixed, config);
    fs::write(&path, &mixed).unwrap();
    let order = synthetic_order(root, &CapacityOrderQuery::default(), &quota_snapshot(false));
    let listed = scores(&order);
    assert_eq!(
        listed[0],
        ("b".to_owned(), serde_json::json!(f64::MAX)),
        "{order}"
    );
    assert!(listed[1].1.as_f64().is_some_and(f64::is_finite), "{order}");
}

/// Historical samples from different sources and their exhaustion latch do
/// not affect admission when the independent Quota service is unavailable.
#[tokio::test]
async fn historical_quota_sources_do_not_drive_admission() {
    for second in [None, Some(50.0)] {
        let temp = ranking_home();
        let root = temp.path();
        fs::write(
            root.join("config.toml"),
            format!(
                "schema_version = 2\n[harnesses.codex]\nbinary = \"/usr/bin/true\"\nhome = \"{r}/codex\"\n[harnesses.claude-code]\nbinary = \"/usr/bin/true\"\nhome = \"{r}/claude\"\n[providers.g]\nharness = \"claude-code\"\nconnection = {{ kind = \"custom\", endpoint = \"https://g.example.com/api\", protocol = \"messages\" }}\nauth_family = \"anthropic\"\nlimits_source = \"none\"\n[[providers.g.models]]\nid = \"m-a\"\n[[providers.g.models]]\nid = \"m-b\"\n[[providers.g.bindings]]\nlabel = \"main\"\naccount = \"acct-a\"\n",
                r = root.display()
            ),
        )
        .unwrap();
        let now = agent_run_core::domain::now();
        let store = agent_run_store::fixture::open(root).unwrap();
        store
            .conn
            .execute("DELETE FROM capacity_samples", [])
            .unwrap();
        for (source, model, remaining, observed) in [
            ("s1", "m-a", Some(0.0), now - 5.0),
            ("s2", "m-b", second, now - 1.0),
        ] {
            store.conn.execute(
                "INSERT INTO capacity_samples(runtime,lane,window,target,source,remaining_percent,reset_at,observed_at,valid_until,payload_json,account_id,quota_key) \
                 VALUES('g','primary','five_hour','acct-a',?1,?2,?3,?4,?5,?6,'acct-a','acct-a::primary')",
                rusqlite::params![source, remaining, now + 3600.0, observed, now + 600.0, json!({"models":[model]}).to_string()],
            ).unwrap();
        }
        store.conn.execute(
            "INSERT INTO quota_exhaustion(account_id,quota_key,source,window_id,observed_at,reset_at) \
             VALUES('acct-a','acct-a::primary','s1','five_hour',?1,?2)",
            rusqlite::params![now - 5.0, now + 3600.0],
        ).unwrap();
        drop(store);

        let writer = common::writer::Harness::start(root);
        let service = Service::new(root.to_path_buf()).with_writer(writer.broker());
        for (model, id) in [("m-a", "adm-a"), ("m-b", "adm-b")] {
            let request = serde_json::from_value(json!({"provider":"g","model":model,
                "profile":"review","task":"task","workdir":root,"request_id":id}))
            .unwrap();
            let admitted = service.admit_provider(request).await.unwrap();
            assert_eq!(admitted["created"], true, "{second:?}");
        }
        let store = agent_run_store::fixture::open(root).unwrap();
        let latches: i64 = store
            .conn
            .query_row(
                "SELECT COUNT(*) FROM quota_exhaustion WHERE source='s1' AND window_id='five_hour'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(latches, 1);
    }
}

/// Current schema-2 provider advice survives a missing writer, retains exact
/// unknown quota standing and privacy, then resumes normal per-session deduplication.
#[tokio::test]
async fn advisory_provider_context_survives_writer_failure_for_both_clients() {
    use agent_run_core::hooks::context;
    for transport in ["codex_queue", "claude_uds"] {
        let home = home();
        let reference = agent_run_domain::domain::OrchestratorRef {
            transport: transport.into(),
            external_session_id: "provider-context".into(),
            external_turn_id: None,
        };
        let fallback = context::build_for_hook(home.path(), &reference, None)
            .await
            .unwrap();
        assert!(fallback.injected);
        assert!(fallback.text.starts_with("Provider capacity order"));
        assert!(fallback.text.contains("\"glm-5.3\":unknown"));
        for private in [
            "private-codex-label",
            "acct-codex",
            "acct-glm",
            "secret-glm",
            "secret-codex",
        ] {
            assert!(!fallback.text.contains(private));
        }
        let writer = common::writer::Harness::start(home.path());
        let _socket = writer.serve_unix(home.path());
        let first = context::build_for_hook(home.path(), &reference, None)
            .await
            .unwrap();
        assert_eq!(first.text, fallback.text);
        let repeated = context::build_for_hook(home.path(), &reference, None)
            .await
            .unwrap();
        assert!(!repeated.injected);
    }
}
