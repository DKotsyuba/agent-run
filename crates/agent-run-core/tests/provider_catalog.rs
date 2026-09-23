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
    // Harnesses without providers is neither a catalog nor the empty one.
    fs::write(
        root.join("config.toml"),
        "schema_version = 2\n[harnesses.codex]\nbinary = '/bin/true'\nhome = '/tmp'\n",
    )
    .unwrap();
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

/// Inserts one account-bound sample on `lane` with explicit freshness.
fn sample(root: &Path, provider: &str, account: &str, lane: &str, remaining: f64, valid: f64) {
    let at = agent_run_core::domain::now();
    agent_run_store::Store::open(root)
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
    let mut store = agent_run_store::Store::open(root).unwrap();
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
    let service = Service::new(temp.path().to_path_buf());
    let all = standing(&service.models(ModelsQuery::default()).await.unwrap());
    assert_eq!(all[0].0, "a", "a-locked has the most capacity: {all:?}");
    let research = service
        .models(ModelsQuery {
            profile: Some("research".into()),
            ..Default::default()
        })
        .await
        .unwrap();
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
    // b-open's only sample expires; a-locked gets an active exhaustion latch.
    agent_run_store::Store::open(root)
        .unwrap()
        .conn
        .execute(
            "UPDATE capacity_samples SET valid_until=observed_at+1 WHERE lane='b-open'",
            [],
        )
        .unwrap();
    let reset = agent_run_core::domain::now() + 7200.0;
    agent_run_store::Store::open(root)
        .unwrap()
        .conn
        .execute(
            "INSERT INTO quota_exhaustion(account_id,quota_key,source,window_id,observed_at,reset_at) \
             VALUES('acct-a','acct-a::a-locked','collector','5h',?1,?2)",
            rusqlite::params![reset - 7300.0, reset],
        )
        .unwrap();
    let service = Service::new(root.to_path_buf());
    let order = service
        .capacity_order(CapacityOrderQuery::default())
        .unwrap();
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
    let missing_catalog = Service::new(missing.path().to_path_buf())
        .models(ModelsQuery::default())
        .await
        .unwrap();
    assert_eq!(
        missing_catalog["providers"][1]["models"][0]["quota"]["evidence"],
        "missing"
    );
}

/// Operator `limits` keeps two accounts sharing one provider lane apart.
#[test]
fn limits_keep_account_bound_rows_distinct() {
    let temp = ranking_home();
    let root = temp.path();
    sample(root, "a", "acct-b", "a-open", 70.0, 600.0);
    let limits = Service::new(root.to_path_buf()).limits().unwrap();
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

/// Registry and quota changes committed between the former separate reads
/// cannot mix: the whole result reflects the one read snapshot.
#[cfg(feature = "test-fixtures")]
#[test]
fn registry_and_quota_come_from_one_committed_read() {
    let temp = home();
    let root = temp.path();
    let (config, revision) = agent_run_config::provider_config::ProviderConfig::load(root).unwrap();
    let before = footprint(root)[4];
    let catalog = agent_run_core::capacity::provider_catalog::models_observed(
        root,
        &config,
        &revision,
        &ModelsQuery::default(),
        &mut || {
            let mut store = agent_run_store::Store::open(root).unwrap();
            // Disabling advances the capacity revision in its own
            // transaction; no separate collector round is needed.
            store.disable_account(&"acct-glm".parse().unwrap()).unwrap();
        },
    )
    .unwrap();
    assert_eq!(catalog["capacity_revision"], before);
    assert_eq!(catalog["providers"][0]["provider"], "glm");
    assert_eq!(
        catalog["providers"][0]["models"][0]["quota"]["status"],
        "available"
    );
    let after = agent_run_core::capacity::provider_catalog::models(
        root,
        &config,
        &revision,
        &ModelsQuery::default(),
    )
    .unwrap();
    assert_eq!(after["capacity_revision"], before + 1);
    let glm = after["providers"]
        .as_array()
        .unwrap()
        .iter()
        .find(|provider| provider["provider"] == "glm")
        .unwrap();
    assert_eq!(glm["models"][0]["quota"]["status"], "no_eligible_account");
}
