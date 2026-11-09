//! Ported capacity-order CLI behaviors with a complete injectable service fake.

use agent_run::{
    cli::{Cli, CliBroker, CliDependencies, CliFuture, CliService},
    domain::AgentId,
    error::invalid,
    service::Query,
};
use clap::Parser;
use serde_json::{Value, json};
use std::{
    path::{Path, PathBuf},
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
    fn models<'a>(&'a self, _query: agent_run_domain::ModelsQuery) -> CliFuture<'a> {
        Box::pin(async { Ok(json!([])) })
    }

    /// Returns an empty limit projection for commands outside this fixture's scope.
    fn limits(&self) -> CliFuture<'_> {
        Box::pin(async { Ok(json!({})) })
    }

    /// Returns the fixed order payload and records one service-shaped call.
    fn capacity_order(&self, _query: agent_run_domain::CapacityOrderQuery) -> CliFuture<'_> {
        Box::pin(async move {
            *self.calls.lock().expect("call counter lock") += 1;
            Ok(self.payload.clone())
        })
    }

    /// Returns empty guide text for commands outside this fixture's scope.
    fn delegation_guide(&self, _query: agent_run_domain::ModelsQuery) -> CliFuture<'_> {
        Box::pin(async { Ok(Value::String(String::new())) })
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
        text_output: Arc::new(|_| Ok(())),
        doctor: Arc::new(|home| {
            Ok(agent_run::doctor::Report {
                home: home.to_owned(),
                checked_at: 0.0,
                findings: Vec::new(),
                ..agent_run::doctor::Report::default()
            })
        }),
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
    agent_run_store::fixture::initialize(home).expect("order store initializes");
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
#[tokio::test]
async fn test_order_rejects_unexpected_arguments() {
    // `--model` is the schema-2 exact model filter; any other flag still fails.
    let parsed = Cli::try_parse_from(["agent-run", "capacity", "order", "--account", "a"]);
    assert!(
        parsed.is_err(),
        "unexpected order flags must fail at parsing"
    );
    assert!(Cli::try_parse_from(["agent-run", "capacity", "order", "--model", "m"]).is_ok());
}

/// Mirrors `tests/test_capacity_cli.py::CapacityOrderCliTests::test_default_facade_serves_capacity_order_without_injection`
#[tokio::test]
async fn test_default_facade_serves_capacity_order_without_injection() {
    let scratch = tempfile::tempdir().expect("capacity home");
    order_config(scratch.path());

    let mut dependencies = CliDependencies::production(PathBuf::from(scratch.path()));
    let output = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&output);
    dependencies.output = Arc::new(move |value| {
        sink.lock().expect("output lock").push(value.clone());
        Ok(())
    });

    let error = agent_run::cli::run_with(
        parse(&[
            "--home",
            scratch.path().to_str().expect("home path"),
            "capacity",
            "order",
        ]),
        dependencies,
    )
    .await
    .expect_err("legacy capacity order requires migration");
    assert_eq!(error.machine_code().as_str(), "Unsupported");
    assert!(error.to_string().contains("schema_version 2"));
    assert!(output.lock().expect("output lock").is_empty());
}
