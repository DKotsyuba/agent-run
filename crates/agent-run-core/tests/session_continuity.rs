//! Session-continuity boundary negatives for a native account change.
//!
//! A native Codex or Claude-family session may only be continued when its
//! recorded grants and runtime identity still hold. These tests drive the
//! real `Service::resume` boundary with temporary homes and store fixtures —
//! no provider, credential store, or real account — and prove the fail-closed
//! negatives an account handoff must satisfy: contradictory grants refuse
//! with an integrity error, and an identity without a sealed runtime home
//! refuses with a validation error. Neither refusal may fall back to a new
//! native conversation. The already-covered halves of the boundary are not
//! duplicated here: session/account pinning and lineage atomicity live in
//! `codex_resume.rs`, and foreign stream identity in `resume_stream.rs`.
//! See `docs/session-continuity.md` for the missing cross-account seam and
//! the pending real A→B proof.

mod common;

use agent_run_core::service::{LaunchIdentity, Service};
use agent_run_core::{policy, profiles};
use agent_run_domain::Error;
use agent_run_domain::domain::{Outcome, StartRequest};
use serde_json::Value;
use std::collections::BTreeSet;

/// Admits, runs, and terminalizes one parent that owns a native session.
///
/// The row is finished with a fixture failure outcome so the durable lineage is
/// terminal, exactly like the resume fixtures in `codex_resume.rs`; the native
/// session string is recorded through the public `runtime_session` boundary,
/// and `identity` is stored verbatim as the row's launch identity.
fn terminal_parent(
    home: &common::Home,
    request: StartRequest,
    session: &str,
    identity: &Value,
) -> agent_run_domain::domain::AgentId {
    let mut store = home.store();
    let (id, created) = store.admit(&request, &home.config, identity, None).unwrap();
    assert!(created);
    store.running(&id, 42).unwrap();
    store.runtime_session(&id, session).unwrap();
    store
        .finish(&id, &Outcome::failure("fixture"), None, None)
        .unwrap();
    id
}

/// Builds a complete Rust launch-identity fixture without credentials.
///
/// Mirrors the identity fixture in `service.rs`: the profile matches the
/// request's role name, write grant, and read roots, and the effective policy
/// is derived from the same public evaluator used at launch. `write_override`
/// flips the recorded write grant to build the tampered variant;
/// `runtime_home` and `snapshot_sha256` are left unsealed because sealing a
/// real runtime snapshot is out of scope for these fixtures.
fn identity(home: &common::Home, request: &StartRequest, write_override: Option<bool>) -> Value {
    let runtime = home.config.runtime(&request.runtime).unwrap();
    let profile = profiles::Profile {
        name: request.profile.clone(),
        body: "fixture".into(),
        write: write_override.unwrap_or(request.write),
        network: false,
        revision: "fixture".into(),
        canonical: false,
        allow_external_read_roots: true,
        read_roots: request.read_roots.clone(),
        skills: vec![],
        mcp: vec![],
        required_constraints: BTreeSet::new(),
    };
    let policy = policy::evaluate(&request.runtime, runtime, &profile);
    serde_json::to_value(LaunchIdentity {
        rust_identity_version: 1,
        replay_request_sha256: None,
        config: home.config.clone(),
        profile,
        effective_policy: policy,
        runtime_home: None,
        snapshot_sha256: None,
    })
    .unwrap()
}

/// Grant tampering and an unsealed runtime home are typed refusals, not resumes.
///
/// Resume must fail closed before any native launch when the recorded grants
/// contradict the request (integrity), and an identity without a sealed runtime
/// home cannot host a native continuation at all (validation). These are the
/// product guards an account handoff must satisfy; a native fallback to a new
/// conversation is never implied by either refusal.
#[tokio::test]
async fn grant_tampering_and_unsealed_home_are_typed_refusals() {
    let home = common::Home::new();
    let mut request = home.request();
    request.account = Some("work".into());
    let sealed = identity(&home, &request, None);
    let parent = terminal_parent(&home, request, "thread-native-grants", &sealed);
    let service = Service::new(home.path.clone());

    // Same identity but with the write grant flipped against the request.
    let tampered = identity(&home, &home.request(), Some(true));
    let store = home.store();
    store
        .conn
        .execute(
            "UPDATE agents SET identity_json=? WHERE id=?",
            (tampered.to_string(), parent.as_str()),
        )
        .unwrap();
    drop(store);
    assert!(
        matches!(
            service
                .resume(&parent, "continue".into(), None, None, None)
                .await,
            Err(Error::Integrity(_))
        ),
        "contradictory grants must refuse resume with an integrity error"
    );

    // Restore the matching identity: resume now stops at the missing sealed
    // runtime home rather than silently starting a new native conversation.
    let store = home.store();
    store
        .conn
        .execute(
            "UPDATE agents SET identity_json=? WHERE id=?",
            (sealed.to_string(), parent.as_str()),
        )
        .unwrap();
    drop(store);
    let error = service
        .resume(&parent, "continue".into(), None, None, None)
        .await
        .expect_err("an unsealed runtime home must not resume");
    assert!(
        matches!(error, Error::Validation(_)),
        "expected a typed validation refusal, got: {error}"
    );
}
