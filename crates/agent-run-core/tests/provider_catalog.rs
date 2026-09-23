//! Public schema-2 provider catalog (`models`) and provider `capacity_order`
//! through the real service and dispatch: exact filters, privacy, revision
//! consistency, read-only semantics, and live recommendation edits.

use agent_run_core::{dispatch, service::Service};
use agent_run_domain::{
    catalog::{AccountRecord, AccountStatus},
    CapacityOrderQuery, ModelsQuery,
};
use serde_json::{json, Value};
use std::{fs, path::Path};

/// Writes the v2 config with `recommendation` on the codex provider.
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
limits_source = "codex_appserver"
recommendations = ["{recommendation}"]
[[providers.codex.models]]
id = "gpt-main"
native_model = "gpt-native"
params = {{ reasoning = "medium" }}
allowed_params = {{ effort = ["medium", "high"] }}
recommendations = ["broad coding"]
[[providers.codex.models]]
id = "gpt-review"
restrictions = ["web_tools_disabled"]
[[providers.codex.bindings]]
label = "personal"
account = "acct-codex"
[providers.glm]
harness = "claude-code"
connection = {{ kind = "custom", endpoint = "https://api.example.com/api", protocol = "messages" }}
auth_family = "anthropic"
limits_source = "lua"
collector = {{ script = "glm_quota", origins = ["https://api.example.com"] }}
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

/// A disposable v2 home with two roles, two accounts, and one fresh sample
/// making `glm-5.3` available while codex stays unobserved.
fn home() -> tempfile::TempDir {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path();
    agent_run_store::Store::initialize(root).unwrap();
    fs::create_dir_all(root.join("profiles")).unwrap();
    fs::write(root.join("profiles/review.md"),
        "+++\nrevision = \"r1\"\nwrite = false\nnetwork = false\nallow_external_read_roots = false\nskills = []\nmcp = []\nrequired_constraints = []\n+++\nReview.\n").unwrap();
    fs::write(root.join("profiles/code.md"),
        "+++\nrevision = \"c1\"\nwrite = true\nnetwork = false\nallow_external_read_roots = false\nskills = []\nmcp = []\nrequired_constraints = []\n+++\nCode.\n").unwrap();
    write_config(root, "native subscription");
    let mut store = agent_run_store::Store::open(root).unwrap();
    for (id, family, reference) in [
        ("acct-codex", "openai", "keychain:secret-codex:ref"),
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
    let store = agent_run_store::Store::open(root).unwrap();
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
        providers[0]["provider"], "glm",
        "known capacity ranks first"
    );
    assert_eq!(providers[0]["models"][0]["quota"]["status"], "available");
    let codex = &providers[1];
    assert_eq!(codex["recommendations"], json!(["native subscription"]));
    assert_eq!(codex["connection"], json!({"kind": "native"}));
    let main = &codex["models"][0];
    assert_eq!(main["native_model"], "gpt-native");
    assert_eq!(main["allowed_params"]["effort"], json!(["medium", "high"]));
    assert_eq!(main["params"]["reasoning"], "medium");
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
        "personal",
        "secret-",
        "keychain",
        "api.example.com",
    ] {
        assert!(!text.contains(private), "{private} leaked: {text}");
    }
    let order = service
        .capacity_order(CapacityOrderQuery::default())
        .unwrap();
    assert_eq!(order["capacity_revision"], before[4]);
    assert!(!order.to_string().contains("acct-"));
    assert_eq!(footprint(root), before, "reads never write");
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
    fs::write(root.join("config.toml"), "schema_version = 2\n").unwrap();
    assert!(service.models(ModelsQuery::default()).await.is_err());
}

/// Schema-1 homes keep their historical reads; new filters are unsupported
/// there rather than silently ignored.
#[tokio::test]
async fn schema_one_filters_are_unsupported() {
    let temp = tempfile::tempdir().unwrap();
    agent_run_store::Store::initialize(temp.path()).unwrap();
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
        .unwrap_err();
    assert_eq!(error.machine_code().as_str(), "Unsupported");
    let value: Value = service
        .capacity_order(CapacityOrderQuery::default())
        .unwrap();
    assert!(value.get("routes").is_some());
}
