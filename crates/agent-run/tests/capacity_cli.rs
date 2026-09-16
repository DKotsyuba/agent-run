//! Ported capacity-order CLI behaviors with a complete injectable service fake.

use agent_run::{
    capacity::{self, Key, Pool, Route, Sample, Slice, Topology},
    cli::{Cli, CliBroker, CliDependencies, CliFuture, CliService},
    domain::{now, AgentId},
    error::invalid,
    service::Query,
    state::Store,
};
use clap::Parser;
use serde_json::{json, Value};
use std::{
    path::{Path, PathBuf},
    process::Command,
    sync::{Arc, Mutex},
};

/// A complete CLI service fake that records capacity-order calls.
struct CapacityServiceFake {
    /// JSON value returned by the read-only order operation.
    payload: Value,
    /// Number of no-argument order calls received.
    calls: Mutex<usize>,
}

impl CapacityServiceFake {
    /// Creates a fake returning payload and initially recording no calls.
    fn new(payload: Value) -> Self {
        Self {
            payload,
            calls: Mutex::new(0),
        }
    }
}

impl CliService for CapacityServiceFake {
    /// Returns a fixed cancellation acknowledgement.
    fn cancel(&self, _id: &AgentId) -> agent_run::Result<Value> {
        Ok(json!({}))
    }

    /// Returns a fixed steering acknowledgement.
    fn steer(&self, _id: &AgentId, _text: &str) -> agent_run::Result<Value> {
        Ok(json!({}))
    }

    /// Returns an empty agent page for commands outside this fixture's scope.
    fn list<'a>(&'a self, _query: Query) -> CliFuture<'a> {
        Box::pin(async { Ok(json!({"items":[]})) })
    }

    /// Returns an empty answer envelope for commands outside this fixture's scope.
    fn answer(&self, _id: &AgentId) -> agent_run::Result<Value> {
        Ok(json!({}))
    }

    /// Returns an empty agent view for commands outside this fixture's scope.
    fn agent(&self, _id: &AgentId) -> agent_run::Result<Value> {
        Ok(json!({}))
    }

    /// Returns an empty transcript page for commands outside this fixture's scope.
    fn transcript(&self, _id: &AgentId, _cursor: i64, _limit: usize) -> agent_run::Result<Value> {
        Ok(json!({}))
    }

    /// Returns an empty model list for commands outside this fixture's scope.
    fn models<'a>(&'a self) -> CliFuture<'a> {
        Box::pin(async { Ok(json!([])) })
    }

    /// Returns an empty limit projection for commands outside this fixture's scope.
    fn limits(&self) -> agent_run::Result<Value> {
        Ok(json!({}))
    }

    /// Returns the fixed order payload and records one service-shaped call.
    fn capacity_order(&self) -> agent_run::Result<Value> {
        *self.calls.lock().expect("call counter lock") += 1;
        Ok(self.payload.clone())
    }

    /// Returns an empty delivery status for commands outside this fixture's scope.
    fn delivery_status(&self, _id: &AgentId) -> agent_run::Result<Value> {
        Ok(json!({}))
    }

    /// Returns an empty delivery cancellation for commands outside this fixture's scope.
    fn delivery_cancel(&self, _id: &str) -> agent_run::Result<Value> {
        Ok(json!({}))
    }
}

/// Broker fake that fails if a capacity-order command unexpectedly uses it.
struct NoopBroker;

impl CliBroker for NoopBroker {
    /// Rejects all broker calls because capacity order is local and read-only.
    fn call<'a>(&'a self, _method: &'a str, _params: Value) -> CliFuture<'a> {
        Box::pin(async { Err(invalid("capacity order unexpectedly used the broker")) })
    }
}

/// Creates parsed CLI arguments from command fragments.
fn parse(args: &[&str]) -> Cli {
    Cli::try_parse_from(std::iter::once("agent-run").chain(args.iter().copied()))
        .expect("fixture CLI parses")
}

/// Creates dependencies whose output values are collected in memory.
fn fake_dependencies(
    service: Arc<CapacityServiceFake>,
    output: Arc<Mutex<Vec<Value>>>,
) -> CliDependencies {
    let sink = Arc::clone(&output);
    CliDependencies {
        service,
        broker: Arc::new(NoopBroker),
        output: Arc::new(move |value| {
            sink.lock().expect("output lock").push(value.clone());
            Ok(())
        }),
        doctor: Arc::new(|home| {
            Ok(agent_run::doctor::Report {
                home: home.to_owned(),
                checked_at: 0.0,
                findings: Vec::new(),
            })
        }),
    }
}

/// Builds one valid route snapshot with a current observation.
fn quota_slice(runtime: &str, remaining: f64, observed: f64) -> Slice {
    let key = Key {
        runtime: runtime.into(),
        lane: format!("{runtime}-lane"),
        window: "window".into(),
        target: None,
        source: "source".into(),
    };
    let pool = Pool {
        pool_id: format!("{runtime}-pool"),
        keys: [key.clone()].into_iter().collect(),
    };
    Slice {
        runtime: runtime.into(),
        scope_id: "scope".into(),
        samples: vec![Sample {
            key,
            remaining_percent: Some(remaining),
            reset_at: Some(observed + 20_000.0),
            observed_at: Some(observed),
            valid_until: Some(observed + 10_000.0),
        }],
        topology: Topology {
            pools: vec![pool.clone()],
            routes: vec![Route {
                route_id: format!("{runtime}-route"),
                runtime: runtime.into(),
                account: None,
                quota_lane: format!("{runtime}-lane"),
                pool_ids: vec![pool.pool_id],
                reset_credits: None,
            }],
        },
        observed_at: observed,
        valid_until: observed + 10_000.0,
    }
}

/// Writes a capacity-order configuration with enabled and disabled runtimes.
fn order_config(home: &Path) {
    let mut text = String::from("schema_version=1\n");
    for (name, enabled) in [
        ("alpha", true),
        ("beta", true),
        ("empty", true),
        ("missing", true),
        ("disabled", false),
    ] {
        text.push_str(&format!(
            "[runtimes.{name}]\nenabled={enabled}\nadapter=\"codex\"\nbinary=\"/bin/true\"\nhome=\"{}\"\nmodels=[\"model\"]\n",
            home.join(format!("{name}-runtime")).display()
        ));
    }
    std::fs::write(home.join("config.toml"), text).expect("order config writes");
    Store::initialize(home).expect("order store initializes");
}

/// Persists one current sample for the named runtime.
fn persist_sample(home: &Path, runtime: &str, remaining: f64, observed: f64) {
    capacity::persist(home, &quota_slice(runtime, remaining, observed), 1000)
        .expect("quota sample persists");
}

/// Returns the runtime names in a JSON route array.
fn route_runtimes(value: &Value) -> Vec<String> {
    value["routes"]
        .as_array()
        .expect("order routes array")
        .iter()
        .map(|route| route["runtime"].as_str().expect("route runtime").into())
        .collect()
}

/// Mirrors `tests/test_capacity_cli.py::CapacityOrderCliTests::test_order_emits_the_service_shape_once`
#[tokio::test]
async fn test_order_emits_the_service_shape_once() {
    let payload = json!({
        "observed_at": 100.0,
        "routes": [{"aliases": [{"runtime": "alpha", "account": "a", "model": "m"}]}],
        "deferred": [],
        "omitted": [{"runtime": "beta", "reason": "exhausted"}],
        "unavailable_runtimes": ["gamma"],
        "insufficient_diversity": true,
    });
    let service = Arc::new(CapacityServiceFake::new(payload.clone()));
    let output = Arc::new(Mutex::new(Vec::new()));

    let code = agent_run::cli::run_with(
        parse(&["capacity", "order"]),
        fake_dependencies(Arc::clone(&service), Arc::clone(&output)),
    )
    .await
    .expect("capacity order runs");

    assert_eq!(code, 0);
    assert_eq!(output.lock().expect("output lock").as_slice(), &[payload]);
    assert_eq!(*service.calls.lock().expect("call counter lock"), 1);
}

/// Mirrors `tests/test_capacity_cli.py::CapacityOrderCliTests::test_order_rejects_unexpected_arguments`
#[test]
fn test_order_rejects_unexpected_arguments() {
    let parsed = Cli::try_parse_from(["agent-run", "capacity", "order", "--model", "m"]);
    assert!(
        parsed.is_err(),
        "unexpected order flags must fail at parsing"
    );
}

/// Mirrors `tests/test_capacity_cli.py::CapacityOrderCliTests::test_real_state_reorders_and_explains_nonworking_runtimes`
#[test]
fn test_real_state_reorders_and_explains_nonworking_runtimes() {
    let scratch = tempfile::tempdir().expect("capacity home");
    order_config(scratch.path());
    let at = now();
    for (runtime, remaining) in [("alpha", 80.0), ("beta", 60.0), ("empty", 0.0)] {
        persist_sample(scratch.path(), runtime, remaining, at);
    }

    let service = agent_run_core::service::Service::new(scratch.path().into());
    let first = service.capacity_order().expect("first order");
    assert_eq!(route_runtimes(&first), vec!["alpha", "beta"]);
    assert_eq!(first["omitted"][0]["runtime"], "empty");
    assert_eq!(first["unavailable_runtimes"], json!(["missing"]));

    persist_sample(scratch.path(), "alpha", 20.0, now());
    persist_sample(scratch.path(), "beta", 90.0, now());
    let second = service.capacity_order().expect("second order");
    assert_eq!(route_runtimes(&second), vec!["beta", "alpha"]);
}

/// Mirrors `tests/test_capacity_cli.py::CapacityOrderCliTests::test_default_facade_serves_capacity_order_without_injection`
#[tokio::test]
async fn test_default_facade_serves_capacity_order_without_injection() {
    let scratch = tempfile::tempdir().expect("capacity home");
    order_config(scratch.path());
    persist_sample(scratch.path(), "alpha", 80.0, now());

    let mut dependencies = CliDependencies::production(PathBuf::from(scratch.path()));
    let output = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&output);
    dependencies.output = Arc::new(move |value| {
        sink.lock().expect("output lock").push(value.clone());
        Ok(())
    });

    let code = agent_run::cli::run_with(
        parse(&[
            "--home",
            scratch.path().to_str().expect("home path"),
            "capacity",
            "order",
        ]),
        dependencies,
    )
    .await
    .expect("default capacity facade runs");

    assert_eq!(code, 0);
    assert_eq!(output.lock().expect("output lock").len(), 1);
    assert_eq!(
        route_runtimes(&output.lock().expect("output lock")[0]),
        vec!["alpha"]
    );
}

/// Mirrors `tests/test_capacity_outcomes.py::CapacityOutcomeRegressionTests::test_capacity_collect_cli_preserves_status_counts_and_degraded_exit`.
#[test]
fn collect_once_prints_the_report_and_returns_degraded_exit_status() {
    let scratch = tempfile::tempdir().expect("capacity home");
    let auth = scratch.path().join("auth.json");
    std::fs::write(&auth, "{}").expect("fixture auth source");
    let missing_home = scratch.path().join("missing-runtime-home");
    std::fs::write(
        scratch.path().join("config.toml"),
        format!(
            "schema_version=1\n[runtimes.codex]\nenabled=true\nadapter=\"codex\"\nbinary=\"/bin/true\"\nhome=\"{}\"\nmodels=[\"fixture\"]\nlimits_source=\"codex_appserver\"\n[runtimes.codex.auth]\nkind=\"file_link\"\nsource=\"{}\"\ntarget=\"auth.json\"\n",
            missing_home.display(),
            auth.display()
        ),
    )
    .expect("collect config writes");
    Store::initialize(scratch.path()).expect("capacity store initializes");

    let output = Command::new(env!("CARGO_BIN_EXE_agent-run"))
        .args([
            "--home",
            scratch.path().to_str().expect("home path"),
            "capacity",
            "collect",
            "--once",
        ])
        .output()
        .expect("collect CLI starts");
    assert_eq!(output.status.code(), Some(2));
    let report: Value = serde_json::from_slice(&output.stdout).expect("printed report");
    assert_eq!(report["ok"], false);
    assert_eq!(report["results"][0]["status"], "failed");
    assert!(
        output.stderr.is_empty(),
        "unexpected stderr: {:?}",
        output.stderr
    );
}
