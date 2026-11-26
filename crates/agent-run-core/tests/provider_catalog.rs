//! Schema-2 provider catalog behind `delegation_guide` through the real
//! service and dispatch: exact filters, privacy, revision consistency,
//! read-only semantics, live recommendation edits, and the absence of any
//! quota presentation (Agent Quota owns it since 0.32.0).

use crate::common;
use agent_run_core::{dispatch, service::Service};
use agent_run_domain::{
    ModelsQuery,
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
        _recover_after_crash: bool,
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

/// Reads the structured account-free catalog the guide renders.
fn catalog(root: &Path, query: &ModelsQuery) -> agent_run_core::Result<Value> {
    let (config, revision) = agent_run_config::provider_config::ProviderConfig::load(root)?;
    let store = agent_run_store::fixture::open(root).unwrap();
    agent_run_core::capacity::provider_catalog::catalog_at(&store, root, &config, &revision, query)
}

/// The unfiltered catalog is one revisioned, account-free snapshot in
/// configured order with explicit offerings and grants, and no quota standing,
/// score or capacity revision.
#[tokio::test]
async fn catalog_is_revisioned_ordered_and_account_free() {
    let temp = home();
    let root = temp.path();
    let before = footprint(root);
    let catalog = catalog(root, &ModelsQuery::default()).unwrap();
    let bytes = fs::read(root.join("config.toml")).unwrap();
    assert_eq!(catalog["schema_version"], 2);
    assert_eq!(
        catalog["config_revision"],
        agent_run_platform::fs::sha256(&bytes)
    );
    let providers = catalog["providers"].as_array().unwrap();
    assert_eq!(providers[0]["provider"], "codex");
    assert_eq!(providers[1]["provider"], "glm");
    let codex = &providers[0];
    assert_eq!(codex["recommendations"], json!(["native subscription"]));
    assert_eq!(codex["connection"], json!({"kind": "native"}));
    let main = &codex["models"][0];
    assert_eq!(main["native_model"], "gpt-native");
    assert_eq!(main["allowed_params"]["effort"], json!(["medium", "high"]));
    assert_eq!(main["params"]["effort"], "medium");
    assert_eq!(main["profiles"], json!(["code", "review"]));
    assert_eq!(
        codex["models"][1]["restrictions"],
        json!(["web_tools_disabled"])
    );
    let text = catalog.to_string();
    for removed in [
        "quota",
        "score",
        "capacity_revision",
        "priority_multiplier",
        "ranked_at",
    ] {
        assert!(!text.contains(removed), "{removed} presented: {text}");
    }
    for private in [
        "acct-",
        "private-codex-label",
        "secret-",
        "keychain",
        "api.example.com",
    ] {
        assert!(!text.contains(private), "{private} leaked: {text}");
    }
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
    let old = catalog(&root, &query).unwrap();
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
    let current = catalog(&root, &query).unwrap();
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
/// typed validation errors, and the removed quota tools are unknown methods.
#[tokio::test]
async fn filters_are_exact_and_typed() {
    let temp = home();
    let root = temp.path();
    let service = Service::new(root.to_path_buf());
    let filtered = catalog(
        root,
        &ModelsQuery {
            provider: Some("codex".into()),
            profile: Some("review".into()),
            model: Some("gpt-main".into()),
        },
    )
    .unwrap();
    let providers = filtered["providers"].as_array().unwrap();
    assert_eq!(providers.len(), 1);
    assert_eq!(providers[0]["models"].as_array().unwrap().len(), 1);
    let by_model = catalog(
        root,
        &ModelsQuery {
            model: Some("glm-5.3".into()),
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(by_model["providers"].as_array().unwrap().len(), 1);
    assert_eq!(by_model["providers"][0]["provider"], "glm");
    for bad in [
        json!({"provider": "missing"}),
        json!({"model": "gpt"}),
        json!({"profile": "missing"}),
        json!({"unknown": 1}),
    ] {
        let error = dispatch::call(&service, "delegation_guide", bad.clone())
            .await
            .unwrap_err();
        assert_eq!(error.machine_code().as_str(), "ValidationError", "{bad}");
    }
    for removed in ["models", "limits", "capacity_order"] {
        assert!(!dispatch::is_tool(removed), "{removed}");
        let error = dispatch::call(&service, removed, json!({}))
            .await
            .unwrap_err();
        assert_eq!(error.machine_code().as_str(), "ValidationError");
        assert!(error.to_string().contains("unknown tool"), "{removed}");
    }
}

/// Editing configured recommendation prose yields a new revision and the new
/// text on the next read; a malformed edit is rejected, never half-applied.
#[tokio::test]
async fn recommendation_edits_are_reflected_by_revision() {
    let temp = home();
    let root = temp.path();
    let service = Service::new(root.to_path_buf());
    let first = catalog(root, &ModelsQuery::default()).unwrap();
    write_config(root, "prefer for long refactors");
    let second = catalog(root, &ModelsQuery::default()).unwrap();
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
    let guide = service.delegation_guide().await.unwrap();
    assert!(
        guide
            .as_str()
            .unwrap()
            .contains("provider guidance: prefer for long refactors")
    );
    // Harnesses without providers is neither a catalog nor the empty one.
    fs::write(
        root.join("config.toml"),
        "schema_version = 2\n[harnesses.codex]\nbinary = '/bin/true'\nhome = '/tmp'\n",
    )
    .unwrap();
    assert!(catalog(root, &ModelsQuery::default()).is_err());
}

/// Schema-1 guide reads, filtered or not, explicitly require migration.
#[tokio::test]
async fn schema_one_filters_are_unsupported() {
    let temp = tempfile::tempdir().unwrap();
    agent_run_store::fixture::initialize(temp.path()).unwrap();
    fs::write(temp.path().join("config.toml"), "schema_version = 1\n").unwrap();
    let service = Service::new(temp.path().to_path_buf());
    let error = service
        .delegation_guide_filtered(ModelsQuery {
            model: Some("x".into()),
            ..Default::default()
        })
        .await
        .unwrap_err();
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

/// A profile filter keeps only offerings the role can use; the restricted
/// offering disappears and the order stays the configured one.
#[tokio::test]
async fn profile_filter_keeps_only_admissible_offerings() {
    let temp = ranking_home();
    let root = temp.path();
    let all = catalog(root, &ModelsQuery::default()).unwrap();
    let names = |value: &Value| -> Vec<(String, Vec<String>)> {
        value["providers"]
            .as_array()
            .unwrap()
            .iter()
            .map(|provider| {
                (
                    provider["provider"].as_str().unwrap().to_owned(),
                    provider["models"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .map(|model| model["model"].as_str().unwrap().to_owned())
                        .collect(),
                )
            })
            .collect()
    };
    assert_eq!(
        names(&all),
        [
            (
                "a".to_owned(),
                vec!["a-locked".to_owned(), "a-open".to_owned()]
            ),
            ("b".to_owned(), vec!["b-open".to_owned()]),
        ]
    );
    let research = catalog(
        root,
        &ModelsQuery {
            profile: Some("research".into()),
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(
        names(&research),
        [
            ("a".to_owned(), vec!["a-open".to_owned()]),
            ("b".to_owned(), vec!["b-open".to_owned()]),
        ]
    );
}

/// The delegation guide renders one committed catalog as compact text:
/// configured order, configured guidance at both levels, admissible
/// profiles, params, restrictions — no quota standing or quota-tool pointer,
/// and no account, credential, or endpoint identity.
#[tokio::test]
async fn delegation_guide_renders_guidance_order_and_privacy() {
    let temp = home();
    let root = temp.path();
    let service = Service::new(root.to_path_buf());
    let guide = service.delegation_guide().await.unwrap();
    let text = guide.as_str().expect("guide is a string result");
    let glm = text.find("provider glm").expect("glm listed");
    let codex = text.find("provider codex").expect("codex listed");
    assert!(codex < glm, "configured order: codex first\n{text}");
    assert!(text.contains("provider glm (harness claude-code)"));
    // Configured recommendation prose at both levels, and only where
    // configured: glm carries no guidance, so nothing is fabricated.
    assert!(text.contains("provider guidance: native subscription"));
    assert!(text.contains("model guidance: broad coding"));
    assert_eq!(text.matches("provider guidance:").count(), 1);
    assert_eq!(text.matches("model guidance:").count(), 1);
    // One short line per exact model id, with no standing attached.
    assert!(text.contains("  - glm-5.3\n"), "{text}");
    assert!(text.contains("  - gpt-main\n"), "{text}");
    assert!(text.contains("  - gpt-review\n"), "{text}");
    // Identical admissible profiles are stated once per provider, not
    // repeated per model.
    assert!(
        text.contains("provider codex (harness codex) — all models admit: code, review"),
        "{text}"
    );
    assert!(text.contains("params: effort=medium; allowed effort: medium|high"));
    assert!(text.contains("restrictions: web_tools_disabled"));
    assert!(text.contains("Agent Quota"), "{text}");
    for removed in [
        "quota available",
        "quota unknown",
        "evidence",
        "sample age",
        "use limits",
        "capacity_order",
        "Provider order reflects capacity",
    ] {
        assert!(!text.contains(removed), "{removed} presented: {text}");
    }
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

/// The UserPromptSubmit context carries no provider capacity block: with no
/// active agents nothing is injected, even when the writer is missing.
#[tokio::test]
async fn hook_context_carries_no_provider_capacity_block() {
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
        assert!(!fallback.injected, "{}", fallback.text);
        assert!(fallback.text.is_empty());
        let writer = common::writer::Harness::start(home.path());
        let _socket = writer.serve_unix(home.path());
        let first = context::build_for_hook(home.path(), &reference, None)
            .await
            .unwrap();
        assert!(!first.injected);
        assert!(!first.text.contains("capacity"), "{}", first.text);
    }
}
