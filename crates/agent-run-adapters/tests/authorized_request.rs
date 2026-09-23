//! Fake-credential checks for scoped, origin-bound custom requests.

use agent_run_adapters::authorized_request::{
    AuthorizedRequest, CredentialReader, SystemCredentialReader,
};
use agent_run_domain::{catalog::ProviderCatalog, CredentialRef, Result};
use reqwest::Method;
use serde_json::json;
use std::os::unix::fs::PermissionsExt;
use std::sync::atomic::{AtomicUsize, Ordering};

/// A Rust-only credential fake that records whether a lookup was attempted.
struct FakeReader(AtomicUsize);

impl CredentialReader for FakeReader {
    /// Returns a synthetic token without reading a host credential store.
    fn read(&self, reference: &CredentialRef) -> Result<String> {
        assert_eq!(reference.kind(), "environment");
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok("synthetic-secret".into())
    }
}

/// Builds one checked custom-provider catalog with a fake reference.
fn catalog() -> ProviderCatalog {
    serde_json::from_value(json!({
        "accounts":[{"account_id":"acct-work","auth_family":"anthropic","secret_ref":"env:FAKE_TOKEN","status":"enabled"}],
        "providers":[{
            "id":"glm","harness":"claude-code",
            "connection":{"kind":"custom","endpoint":"https://api.example.com/base","protocol":"messages"},
            "auth_family":"anthropic","limits_source":"lua",
            "collector":{"script":"glm_quota","origins":["https://api.example.com"]},
            "models":[{"id":"glm-5.3","native_model":"glm-5.3[1m]"}],
            "bindings":[{"label":"work","account":"acct-work"}]
        }]
    })).unwrap()
}

/// Only an enabled in-scope account can authorize the configured origin;
/// rejected origins do not even read the fake credential.
#[test]
fn capability_binds_account_and_origin_without_exposing_a_token() {
    let catalog = catalog();
    let capability = AuthorizedRequest::new(
        &catalog,
        &"glm".parse().unwrap(),
        "glm-5.3",
        &"acct-work".parse().unwrap(),
    )
    .unwrap();
    let fake = FakeReader(AtomicUsize::new(0));
    for rejected in [
        "https://elsewhere.example/quota",
        "https://api.example.com:444/quota",
        "https://user@api.example.com/quota",
        "https://api.example.com/quota#fragment",
    ] {
        assert!(capability
            .prepare_request(Method::GET, rejected, &fake)
            .is_err());
    }
    assert_eq!(fake.0.load(Ordering::SeqCst), 0);
    let request = capability
        .prepare_request(Method::GET, "https://api.example.com/quota", &fake)
        .unwrap();
    assert_eq!(
        request.headers()["authorization"],
        "Bearer synthetic-secret"
    );
    assert!(request.headers()["authorization"].is_sensitive());
    assert!(!format!("{request:?}").contains("synthetic-secret"));
    assert_eq!(fake.0.load(Ordering::SeqCst), 1);
    assert!(AuthorizedRequest::new(
        &catalog,
        &"glm".parse().unwrap(),
        "missing",
        &"acct-work".parse().unwrap()
    )
    .is_err());
    assert!(AuthorizedRequest::new(
        &catalog,
        &"glm".parse().unwrap(),
        "glm-5.3",
        &"acct-other".parse().unwrap()
    )
    .is_err());

    let mut disabled = serde_json::to_value(&catalog).unwrap();
    disabled["accounts"][0]["status"] = json!("disabled");
    let disabled: ProviderCatalog = serde_json::from_value(disabled).unwrap();
    assert!(AuthorizedRequest::new(
        &disabled,
        &"glm".parse().unwrap(),
        "glm-5.3",
        &"acct-work".parse().unwrap()
    )
    .is_err());
}

/// Header style is explicit and native login never becomes an HTTP token.
#[test]
fn gateway_header_and_native_login_remain_distinct() {
    let mut wire = serde_json::to_value(catalog()).unwrap();
    wire["providers"][0]["connection"]["auth_header"] = json!("x_api_key");
    let catalog: ProviderCatalog = serde_json::from_value(wire.clone()).unwrap();
    let capability = AuthorizedRequest::new(
        &catalog,
        &"glm".parse().unwrap(),
        "glm-5.3",
        &"acct-work".parse().unwrap(),
    )
    .unwrap();
    let request = capability
        .prepare_request(
            Method::GET,
            "https://api.example.com/quota",
            &FakeReader(AtomicUsize::new(0)),
        )
        .unwrap();
    assert_eq!(request.headers()["x-api-key"], "synthetic-secret");
    wire["accounts"][0]["secret_ref"] = json!("native:claude-code");
    let catalog: ProviderCatalog = serde_json::from_value(wire).unwrap();
    assert!(AuthorizedRequest::new(
        &catalog,
        &"glm".parse().unwrap(),
        "glm-5.3",
        &"acct-work".parse().unwrap()
    )
    .is_err());
}

/// The system reader handles an existing file without persisting its bytes.
#[test]
fn file_reference_reads_only_at_request_time() {
    let home = tempfile::tempdir().unwrap();
    let path = home.path().join("fake-auth");
    std::fs::write(&path, "synthetic-secret\n").unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
    let reference: CredentialRef = format!("file:{}", path.display()).parse().unwrap();
    assert_eq!(
        SystemCredentialReader.read(&reference).unwrap(),
        "synthetic-secret"
    );
}
