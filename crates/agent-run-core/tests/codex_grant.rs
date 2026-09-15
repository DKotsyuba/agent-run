use agent_run_core::codex::Grant;
use serde_json::{json, Value};

/// Ensures an engine echo cannot widen a read-only Codex grant's network access.
#[test]
fn codex_echo_cannot_widen_network() {
    let grant = Grant {
        model: "fixture".into(),
        cwd: "/workspace".into(),
        roots: vec!["/workspace".into()],
        writable_roots: vec![],
        sandbox: "read-only".into(),
        approval_policy: "never".into(),
        reviewer: None,
        network_access: false,
        permission_profile: None,
    };
    let mut echo = json!({"model":"fixture","cwd":"/workspace","roots":["/workspace"],"sandbox":{"type":"readOnly","networkAccess":false},"approvalPolicy":"never"});
    assert!(grant.verify(&echo).is_ok());
    echo["sandbox"]["networkAccess"] = Value::Bool(true);
    assert!(grant.verify(&echo).is_err());
}
