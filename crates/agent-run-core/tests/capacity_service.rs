//! Capacity service parity tests for runtime filtering and JSON dispatch.
mod common;

use agent_run_core::{dispatch, service::Service};
use agent_run_domain::domain::now;
use rusqlite::params;
use serde_json::{json, Value};
use std::cell::Cell;

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

/// Mirrors `tests/test_capacity_service.py::CapacityServiceTests::test_service_reads_clock_once_and_filters_runtime_availability`
#[test]
fn service_reads_clock_once_and_filters_runtime_availability() {
    let home = common::Home::new();
    config(&home);
    seed(&home, "enabled", 60.0, &["route"]);
    seed(&home, "disabled", 90.0, &["disabled-route"]);
    let calls = Cell::new(0);
    let result = agent_run_core::capacity::order_with_clock(&home.path, || {
        calls.set(calls.get() + 1);
        now()
    })
    .unwrap();
    assert_eq!(calls.get(), 1);
    assert_eq!(
        result["routes"].as_array().unwrap()[0]["runtime"],
        "enabled"
    );
    assert!(result["unavailable_runtimes"]
        .as_array()
        .unwrap()
        .iter()
        .any(|value| value == "empty"));
    assert!(!result.to_string().contains("disabled"));
}

/// Mirrors `tests/test_capacity_service.py::CapacityServiceTests::test_dispatch_serializes_routes_and_current_limits`
#[tokio::test]
async fn dispatch_serializes_routes_and_current_limits() {
    let home = common::Home::new();
    config(&home);
    seed(&home, "enabled", 20.0, &["route-a", "route-b"]);
    let service = Service::new(home.path);
    let order = dispatch::call(&service, "capacity_order", json!({}))
        .await
        .unwrap();
    assert_eq!(order["routes"][0]["runtime"], "enabled");
    assert_eq!(order["routes"][0]["aliases"].as_array().unwrap().len(), 2);
    let limits = dispatch::call(&service, "limits", json!({})).await.unwrap();
    assert!(limits["items"].is_array());
    assert!(limits["items"][0].get("remaining_percent").is_some());
}
