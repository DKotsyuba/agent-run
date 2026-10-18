//! Frozen paid-probe admission and strict private callback compatibility.
use agent_run_domain::{
    ProviderStartRequest,
    domain::StartRequest,
    health::{PingPolicy, PingReply},
};
use serde_json::json;

/// Minimal public provider input with no real credentials or model invocation.
fn request() -> serde_json::Value {
    json!({"provider":"fixture","model":"fixture","profile":"review","task":"task","workdir":"/"})
}

/// Default observation adds no request field or paid permission; initialization
/// markers, rather than rewritten history, select newly monitored attempts.
#[test]
fn default_observation_preserves_historical_request_bytes() {
    let public: ProviderStartRequest = serde_json::from_value(request()).unwrap();
    assert_eq!(public.idle_ping, PingPolicy::Observe);
    let serialized = serde_json::to_value(&public).unwrap();
    assert!(serialized.get("idle_ping").is_none());
    assert_eq!(
        ProviderStartRequest::from_history(serialized.clone()).unwrap(),
        public
    );
    let historical = ProviderStartRequest::from_history(request()).unwrap();
    assert_eq!(historical.idle_ping, PingPolicy::Observe);
    let storage = historical.storage_projection();
    assert!(
        serde_json::to_value(storage)
            .unwrap()
            .get("idle_ping")
            .is_none()
    );
    let legacy = StartRequest::from_history(
        json!({"runtime":"codex","model":"fixture","profile":"review","task":"task","workdir":"/"}),
    )
    .unwrap();
    assert_eq!(legacy.idle_ping, PingPolicy::Observe);
}

/// Paid recovery is a deliberate frozen input, refused for legacy completion.
#[test]
fn recovery_requires_explicit_finish_and_strict_policy() {
    let mut input = request();
    input["idle_ping"] = json!("recover");
    input["explicit_finish"] = json!(false);
    let mut request: ProviderStartRequest = serde_json::from_value(input.clone()).unwrap();
    assert!(request.validate_intent().is_err());
    input["explicit_finish"] = json!(true);
    let mut request: ProviderStartRequest = serde_json::from_value(input.clone()).unwrap();
    request.validate_intent().unwrap();
    assert_eq!(request.storage_projection().idle_ping, PingPolicy::Recover);
    input["idle_ping"] = json!("always");
    assert!(serde_json::from_value::<ProviderStartRequest>(input).is_err());
}

/// Worker replies cannot select another agent, carry prose or use unbounded ids.
#[test]
fn health_reply_is_closed_and_bounded() {
    let good = json!({"ping_id":"a".repeat(64),"state":"waiting"});
    let reply: PingReply = serde_json::from_value(good.clone()).unwrap();
    reply.validate().unwrap();
    for value in [
        "A".repeat(64),
        "z".repeat(64),
        "a".repeat(65),
        String::new(),
    ] {
        let mut input = good.clone();
        input["ping_id"] = json!(value);
        assert!(
            serde_json::from_value::<PingReply>(input)
                .unwrap()
                .validate()
                .is_err()
        );
    }
    let mut input = good;
    input["agent_id"] = json!("foreign");
    assert!(serde_json::from_value::<PingReply>(input).is_err());
}

/// Version three adds only the private reply; old v1/v2 catalogs keep their exact
/// tool bytes and remain recognized, without falsely certifying a v2 health tool.
#[test]
fn health_catalog_preserves_legacy_and_research_grants() {
    use agent_run_domain::worker::*;
    assert!(
        !agent_run_domain::tools::worker_tools_json()
            .iter()
            .any(|t| t["name"] == "health_reply")
    );
    for research in [false, true] {
        let current = finish_tools_json(research);
        assert_eq!(
            current
                .iter()
                .filter(|t| t["name"] == "health_reply")
                .count(),
            1
        );
        let old: Vec<_> = current
            .into_iter()
            .filter(|t| t["name"] != "health_reply")
            .collect();
        let digest = agent_run_domain::canonical::sha256_hex(&json!(old), true);
        assert!(known_pool_catalog(2, &digest));
        assert!(!known_pool_catalog(3, &digest));
        assert!(known_pool_catalog(3, &finish_catalog_digest(research)));
    }
}
