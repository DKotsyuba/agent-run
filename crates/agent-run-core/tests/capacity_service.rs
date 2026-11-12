//! Capacity service parity tests for runtime filtering and JSON dispatch.
use crate::common;

use agent_run_core::{dispatch, service::Service};
use agent_run_domain::domain::now;
use rusqlite::params;
use serde_json::{Value, json};

/// Replaces the fixture config with the three runtime states used by the service contract.
fn config(home: &common::Home) {
    let binary = if std::path::Path::new("/usr/bin/true").is_file() {
        "/usr/bin/true"
    } else {
        "/bin/true"
    };
    let text = format!(
        "schema_version=1\n[runtimes.enabled]\nenabled=true\nadapter='claude'\nbinary='{binary}'\nhome='{}/enabled'\nmodels=['model']\n[runtimes.empty]\nenabled=true\nadapter='claude'\nbinary='{binary}'\nhome='{}/empty'\nmodels=['model']\n[runtimes.disabled]\nenabled=false\nadapter='claude'\nbinary='{binary}'\nhome='{}/disabled'\nmodels=['model']\n",
        home.path.display(),
        home.path.display(),
        home.path.display()
    );
    std::fs::write(home.path.join("config.toml"), text).unwrap();
}

/// Persists one fresh sample and its route topology for `runtime`.
fn seed(home: &common::Home, runtime: &str, remaining: f64, route_ids: &[&str]) {
    let at = now();
    let key = json!({
        "runtime": runtime,
        "lane": "lane",
        "window": "window",
        "target": null,
        "source": "source"
    });
    let topology = json!({
        "pools": [{"pool_id": format!("{runtime}-pool"), "keys": [key]}],
        "routes": route_ids.iter().enumerate().map(|(index, id)| json!({
            "route_id": id,
            "runtime": runtime,
            "account": format!("account-{index}"),
            "quota_lane": format!("quota-{index}"),
            "pool_ids": [format!("{runtime}-pool")]
        })).collect::<Vec<Value>>()
    });
    let connection = rusqlite::Connection::open(home.path.join("state.db")).unwrap();
    connection.execute(
        "INSERT INTO capacity_samples(runtime,lane,window,target,source,remaining_percent,reset_at,observed_at,valid_until,payload_json) VALUES(?,?,?,?,?,?,?,?,?,?)",
        params![runtime, "lane", "window", Option::<String>::None, "source", remaining, at + 1000.0, at - 1.0, at + 100.0, "null"],
    ).unwrap();
    connection.execute(
        "INSERT INTO capacity_route_snapshots(runtime,scope_id,observed_at,valid_until,payload_json) VALUES(?,?,?,?,?)",
        params![runtime, "fresh", at - 1.0, at + 100.0, serde_json::to_string(&topology).unwrap()],
    ).unwrap();
}

/// Legacy quota facts cannot bypass the schema-2 migration requirement.
#[tokio::test]
async fn dispatch_requires_schema_two_even_with_legacy_quota_facts() {
    let home = common::Home::new();
    config(&home);
    seed(&home, "enabled", 20.0, &["route-a", "route-b"]);
    let service = Service::new(home.path.clone());
    for method in ["capacity_order", "limits"] {
        let error = dispatch::call(&service, method, json!({}))
            .await
            .unwrap_err();
        assert!(
            matches!(&error, agent_run_domain::Error::Unsupported(_)),
            "{method}: {error}"
        );
        assert!(
            error.to_string().contains("schema_version 2"),
            "{method}: {error}"
        );
    }
}

/// Legacy runtime configuration still resolves its own explicit priority overrides.
#[test]
fn route_weight_precedence_is_account_then_lane_then_runtime_default() {
    let h = common::Home::new();
    let mut runtime = h.config.runtime("mock").unwrap().clone();
    runtime.priority_multiplier = 1.0;
    runtime
        .priority_lane_multipliers
        .insert("new-lane".into(), 2.0);
    runtime
        .priority_account_multipliers
        .insert("new-account".into(), 3.0);
    assert_eq!(runtime.weight(Some("new-account"), "new-lane"), 3.0);
    assert_eq!(runtime.weight(None, "new-lane"), 2.0);
    assert_eq!(runtime.weight(None, "other-lane"), 1.0);
    assert_eq!(runtime.weight(Some("unknown-account"), "new-lane"), 2.0);
}
