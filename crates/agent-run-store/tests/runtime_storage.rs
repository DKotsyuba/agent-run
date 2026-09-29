//! Durable runtime storage-layout registry: contract, pins, recovery, removal.

mod common;

use agent_run_domain::{domain::Outcome, Error};
use agent_run_store::{
    runtime_storage::{LayoutState, RuntimeStorageLayout},
    Store,
};
use serde_json::{json, Value};

/// A canonical version-1 layout for `home` mapping the given managed roots.
fn layout(home: &str, roots: Value) -> String {
    serde_json::to_string(&json!({
        "version": 1,
        "runtime_home": home,
        "index_sha256": "1".repeat(64),
        "roots": roots,
    }))
    .unwrap()
}

/// One mapping of a managed root into the shared store.
fn mapping() -> Value {
    json!({"scope": "2".repeat(64), "manifest_sha256": "3".repeat(64)})
}

/// One initialized disposable store home.
fn home() -> common::Home {
    common::Home::new()
}

/// Malformed, non-canonical and out-of-contract layouts are refused without
/// touching the registry.
#[test]
fn invalid_layouts_are_refused() {
    let home = home();
    let mut store = home.store();
    let valid = layout("/runtime/alpha", json!({"assets": mapping()}));
    for bad in [
        // Not JSON at all.
        "not json".to_owned(),
        // Unknown field and unknown version are both hard refusals.
        layout("/runtime/alpha", json!({"assets": mapping()}))
            .replace("\"version\":1", "\"version\":2,\"extra\":1"),
        // Non-lowercase digests and unsafe homes.
        layout("/runtime/alpha", json!({"assets": mapping()}))
            .replace(&"1".repeat(64), &"A".repeat(64)),
        layout("runtime/alpha", json!({"assets": mapping()})),
        layout("/runtime/../alpha", json!({"assets": mapping()})),
        layout("/runtime//alpha", json!({"assets": mapping()})),
        // Unsafe, overlapping, duplicated and empty root sets.
        layout("/runtime/alpha", json!({"../assets": mapping()})),
        layout("/runtime/alpha", json!({"/assets": mapping()})),
        layout(
            "/runtime/alpha",
            json!({"assets/plugins": mapping(), "assets": mapping()}),
        ),
        layout(
            "/runtime/alpha",
            json!({"assets": mapping(), "assets/cache": mapping()}),
        ),
        layout("/runtime/alpha", json!({"a/../b": mapping()})),
        // `a` overlaps `a/b` even when `a-b` sorts between them.
        layout(
            "/runtime/alpha",
            json!({"a": mapping(), "a-b": mapping(), "a/b": mapping()}),
        ),
        layout("/runtime/alpha", json!({})),
        // Valid contract but not the canonical encoding.
        layout("/runtime/alpha", json!({"assets": mapping()})).replace(",\"roots\"", ", \"roots\""),
        // A duplicate root collapses to one entry, so the text is not canonical.
        format!(
            "{{\"index_sha256\":\"{}\",\"roots\":{{\"assets\":{},\"assets\":{}}},\
             \"runtime_home\":\"/runtime/alpha\",\"version\":1}}",
            "1".repeat(64),
            mapping(),
            mapping()
        ),
    ] {
        let error = store
            .prepare_runtime_storage_layout(&bad, None)
            .unwrap_err();
        assert!(
            matches!(error, Error::Validation(ref message) if !message.is_empty()),
            "{bad}: {error:?}"
        );
        assert!(store
            .runtime_storage_layout("/runtime/alpha")
            .unwrap()
            .is_none());
    }
    // The one valid form is accepted and parses back to its own contract.
    let record = store.prepare_runtime_storage_layout(&valid, None).unwrap();
    assert_eq!(record.state, LayoutState::Prepared);
    assert_eq!(
        record.layout(),
        &serde_json::from_str::<RuntimeStorageLayout>(&valid).unwrap()
    );
    // Retrying the exact same layout is idempotent: same row, same token.
    let retry = store.prepare_runtime_storage_layout(&valid, None).unwrap();
    assert_eq!(retry.operation_token, record.operation_token);
    assert_eq!(retry.updated_at, record.updated_at);
    // A different layout for the same home is refused, never replaced.
    let other = layout("/runtime/alpha", json!({"cache": mapping()}));
    assert!(matches!(
        store
            .prepare_runtime_storage_layout(&other, None)
            .unwrap_err(),
        Error::Conflict
    ));
    assert_eq!(
        store
            .runtime_storage_layout("/runtime/alpha")
            .unwrap()
            .unwrap()
            .layout_sha256,
        record.layout_sha256
    );
    // A committed mapping is never re-opened by preparing over it.
    store
        .commit_runtime_storage_layout(
            "/runtime/alpha",
            &record.operation_token,
            &record.layout_sha256,
        )
        .unwrap();
    assert!(matches!(
        store
            .prepare_runtime_storage_layout(&valid, None)
            .unwrap_err(),
        Error::Conflict
    ));
    assert_eq!(
        store
            .runtime_storage_layout("/runtime/alpha")
            .unwrap()
            .unwrap()
            .state,
        LayoutState::Committed
    );
}

/// Commit is a compare-and-swap on the exact token and digest: retries of the
/// same values are idempotent and anything else is refused without changes.
#[test]
fn commit_is_exact_and_idempotent() {
    let home = home();
    let mut store = home.store();
    let text = layout("/runtime/alpha", json!({"assets": mapping()}));
    let record = store.prepare_runtime_storage_layout(&text, None).unwrap();
    for (token, digest) in [
        ("wrong-token-but-right-digest", record.layout_sha256.clone()),
        (record.operation_token.as_str(), "0".repeat(64)),
    ] {
        assert!(matches!(
            store
                .commit_runtime_storage_layout("/runtime/alpha", token, &digest)
                .unwrap_err(),
            Error::Conflict
        ));
        assert_eq!(
            store
                .runtime_storage_layout("/runtime/alpha")
                .unwrap()
                .unwrap()
                .state,
            LayoutState::Prepared
        );
    }
    let committed = store
        .commit_runtime_storage_layout(
            "/runtime/alpha",
            &record.operation_token,
            &record.layout_sha256,
        )
        .unwrap();
    assert_eq!(committed.state, LayoutState::Committed);
    let again = store
        .commit_runtime_storage_layout(
            "/runtime/alpha",
            &record.operation_token,
            &record.layout_sha256,
        )
        .unwrap();
    assert_eq!(again.state, LayoutState::Committed);
    // A row whose stored bytes no longer match its stored digest is an
    // integrity failure, never a mapping handed back to a caller.
    store
        .conn
        .execute(
            "UPDATE runtime_storage_layouts SET layout_json=? WHERE runtime_home=?",
            rusqlite::params![
                layout("/runtime/alpha", json!({"cache": mapping()})),
                "/runtime/alpha"
            ],
        )
        .unwrap();
    assert!(matches!(
        store.runtime_storage_layout("/runtime/alpha").unwrap_err(),
        Error::Integrity(_)
    ));
    // An unregistered home is a validation error, not a conflict.
    assert!(matches!(
        store
            .commit_runtime_storage_layout(
                "/runtime/none",
                &record.operation_token,
                &"0".repeat(64)
            )
            .unwrap_err(),
        Error::Validation(_)
    ));
}

/// A home an unresolved agent still uses cannot be prepared, the recorded
/// owner may register or idempotently retry its own layout only while it
/// proves nothing live, and neither a different layout nor a committed row
/// ever replaces what is registered.
#[test]
fn prepare_refuses_pinned_homes_and_allows_owner_recovery() {
    let home = home();
    let runtime_home = home.path.join("runtime").to_string_lossy().to_string();
    let identity = json!({"runtime_home": runtime_home});
    let mut store = home.store();
    let (parent, _) = store
        .admit(&home.request(), &home.config, &identity, None)
        .unwrap();
    let text = layout(&runtime_home, json!({"assets": mapping()}));
    // The admitted agent is starting, so its home is unresolved and pinned.
    assert!(matches!(
        store
            .prepare_runtime_storage_layout(&text, None)
            .unwrap_err(),
        Error::Conflict
    ));
    // A different agent may not register on the holder's behalf.
    let stranger = agent_run_domain::domain::AgentId::new();
    assert!(matches!(
        store
            .prepare_runtime_storage_layout(&text, Some(&stranger))
            .unwrap_err(),
        Error::Conflict
    ));
    // The holder itself may: it never opened an attempt, so it holds nothing
    // live on its own home.
    let record = store
        .prepare_runtime_storage_layout(&text, Some(&parent))
        .unwrap();
    assert_eq!(record.owner_agent_id.as_deref(), Some(parent.as_str()));
    // The exact retry is idempotent, and a different layout never replaces it.
    let retry = store
        .prepare_runtime_storage_layout(&text, Some(&parent))
        .unwrap();
    assert_eq!(retry.operation_token, record.operation_token);
    assert!(matches!(
        store
            .prepare_runtime_storage_layout(
                &layout(&runtime_home, json!({"cache": mapping()})),
                Some(&parent)
            )
            .unwrap_err(),
        Error::Conflict
    ));
    assert!(matches!(
        store
            .prepare_runtime_storage_layout(&text, None)
            .unwrap_err(),
        Error::Conflict
    ));
    // A claimed spawn with no inspected process identity yet is still live,
    // and so is a released attempt whose cleanup was never verified.
    let attempt = format!("att_{}", uuid::Uuid::new_v4().simple());
    store
        .conn
        .execute(
            "INSERT INTO attempts(id,agent_id,number,state,adapter_state_json,created_at,phase,\
             process_identity,ownership_active) \
             VALUES(?,?,1,'starting','{}',1.0,'spawning',NULL,1)",
            rusqlite::params![attempt, parent.as_str()],
        )
        .unwrap();
    assert!(matches!(
        store
            .prepare_runtime_storage_layout(&text, Some(&parent))
            .unwrap_err(),
        Error::Conflict
    ));
    store
        .conn
        .execute(
            "UPDATE attempts SET ownership_active=0,phase='stopping',process_identity='p' WHERE id=?",
            [&attempt],
        )
        .unwrap();
    assert!(matches!(
        store
            .prepare_runtime_storage_layout(&text, Some(&parent))
            .unwrap_err(),
        Error::Conflict
    ));
    // Verified cleanup releases the hold; the retry returns the same row.
    store
        .conn
        .execute(
            "UPDATE attempts SET phase='cleanup_complete',cleanup_proof_json='{\"confirmed\":true}' \
             WHERE id=?",
            [&attempt],
        )
        .unwrap();
    let settled = store
        .prepare_runtime_storage_layout(&text, Some(&parent))
        .unwrap();
    assert_eq!(settled.operation_token, record.operation_token);
    // A proof column that does not parse to a positive verdict is no proof:
    // an empty object, an explicit false and a null phase all keep holding.
    for proof in ["{}", "{\"confirmed\":false}", "null"] {
        store
            .conn
            .execute(
                "UPDATE attempts SET phase=CASE ?2 WHEN 'null' THEN NULL ELSE 'cleanup_complete' END,\
                 cleanup_proof_json=?1 WHERE id=?3",
                rusqlite::params![proof, proof, attempt],
            )
            .unwrap();
        assert!(
            matches!(
                store
                    .prepare_runtime_storage_layout(&text, Some(&parent))
                    .unwrap_err(),
                Error::Conflict
            ),
            "{proof} must keep holding the home"
        );
    }
    // The owner's consolidation window: still `running`, but every attempt
    // carries a confirmed cleanup proof, so its own retry is allowed.
    store
        .conn
        .execute(
            "UPDATE attempts SET phase='cleanup_complete',\
             cleanup_proof_json='{\"confirmed\":true}' WHERE id=?",
            [&attempt],
        )
        .unwrap();
    store
        .conn
        .execute(
            "UPDATE agents SET status='running' WHERE id=?",
            [parent.as_str()],
        )
        .unwrap();
    assert_eq!(
        store
            .prepare_runtime_storage_layout(&text, Some(&parent))
            .unwrap()
            .operation_token,
        record.operation_token
    );
    // A merely `prepared` attempt never satisfies a running owner: that is
    // the account-switch spawn window.
    let second = format!("att_{}", uuid::Uuid::new_v4().simple());
    store
        .conn
        .execute(
            "INSERT INTO attempts(id,agent_id,number,state,adapter_state_json,created_at,phase,\
             ownership_active) VALUES(?,?,2,'starting','{}',2.0,'prepared',0)",
            rusqlite::params![second, parent.as_str()],
        )
        .unwrap();
    assert!(matches!(
        store
            .prepare_runtime_storage_layout(&text, Some(&parent))
            .unwrap_err(),
        Error::Conflict
    ));
    let listed = store.pending_runtime_storage_layouts(10).unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].runtime_home, runtime_home);
}

/// A prepared layout blocks a legacy continuation admission in the same
/// transaction; a committed layout admits.
#[test]
fn prepared_layout_blocks_legacy_resume() {
    let home = home();
    let runtime_home = home.path.join("runtime").to_string_lossy().to_string();
    let identity = json!({"runtime_home": runtime_home});
    let mut store = home.store();
    let (parent, _) = store
        .admit(&home.request(), &home.config, &identity, None)
        .unwrap();
    store.runtime_session(&parent, "fixture-session").unwrap();
    store
        .finish(&parent, &Outcome::failure("fixture"), None, None)
        .unwrap();
    let parent = store.get(&parent).unwrap();
    let text = layout(&runtime_home, json!({"assets": mapping()}));
    let prepared = store.prepare_runtime_storage_layout(&text, None).unwrap();
    let blocked = store
        .admit(&home.request(), &home.config, &json!({}), Some(&parent))
        .unwrap_err();
    assert!(
        matches!(&blocked, Error::Unsupported(message) if message.contains("prepared storage layout")),
        "{blocked:?}"
    );
    assert_eq!(agents(&store), 1);
    // Age never retires a prepared row.
    store
        .conn
        .execute("UPDATE runtime_storage_layouts SET updated_at=0.0", [])
        .unwrap();
    assert_eq!(store.pending_runtime_storage_layouts(10).unwrap().len(), 1);
    assert!(matches!(
        store
            .admit(&home.request(), &home.config, &json!({}), Some(&parent))
            .unwrap_err(),
        Error::Unsupported(_)
    ));
    store
        .commit_runtime_storage_layout(
            &runtime_home,
            &prepared.operation_token,
            &prepared.layout_sha256,
        )
        .unwrap();
    let (child, created) = store
        .admit(&home.request(), &home.config, &json!({}), Some(&parent))
        .unwrap();
    assert!(created);
    assert_eq!(agents(&store), 2);
    assert!(store.get(&child).unwrap().parent_agent_id.is_some());
}

/// A registry row survives its agent history and is removed only by an
/// explicit call that first proves no agent still references the home.
#[test]
fn rows_survive_history_and_removal_requires_no_references() {
    let home = home();
    let runtime_home = home.path.join("runtime").to_string_lossy().to_string();
    let identity = json!({"runtime_home": runtime_home});
    let mut store = home.store();
    let (parent, _) = store
        .admit(&home.request(), &home.config, &identity, None)
        .unwrap();
    store
        .finish(&parent, &Outcome::failure("fixture"), None, None)
        .unwrap();
    let text = layout(&runtime_home, json!({"assets": mapping()}));
    let record = store.prepare_runtime_storage_layout(&text, None).unwrap();
    store
        .commit_runtime_storage_layout(
            &runtime_home,
            &record.operation_token,
            &record.layout_sha256,
        )
        .unwrap();
    // History expiry removes the agent rows; the mapping row stays.
    store
        .conn
        .execute("DELETE FROM agents WHERE parent_agent_id IS NOT NULL", [])
        .unwrap();
    for table in ["events", "run_stats"] {
        store
            .conn
            .execute(
                &format!("DELETE FROM {table} WHERE agent_id=?"),
                [parent.as_str()],
            )
            .unwrap();
    }
    store
        .conn
        .execute("DELETE FROM agents WHERE id=?", [parent.as_str()])
        .unwrap();
    assert_eq!(
        store
            .runtime_storage_layout(&runtime_home)
            .unwrap()
            .unwrap()
            .state,
        LayoutState::Committed
    );
    // While any agent row still binds the home, removal demands the caller's
    // physical-home-gone, configuration and service-reference proof.
    let mut blocked = home.store();
    blocked
        .conn
        .execute(
            "INSERT INTO agents(id,runtime,model,profile,task,task_summary,workdir,request_json,\
             status,created_at,timeout_seconds,config_revision,identity_json) \
             VALUES('agt_ref','mock','fixture','review','task','summary','/tmp','{}','failed',1.0,\
             10.0,'cfg',?)",
            rusqlite::params![identity.to_string()],
        )
        .unwrap();
    let refused = blocked
        .remove_runtime_storage_layout(&runtime_home)
        .unwrap_err();
    assert!(
        matches!(&refused, Error::Validation(message) if message.contains("still referenced")),
        "{refused:?}"
    );
    assert!(blocked
        .runtime_storage_layout(&runtime_home)
        .unwrap()
        .is_some());
    // Once the last reference is gone the explicit removal succeeds once.
    blocked
        .conn
        .execute("DELETE FROM agents WHERE id='agt_ref'", [])
        .unwrap();
    assert!(store.remove_runtime_storage_layout(&runtime_home).unwrap());
    assert!(!store.remove_runtime_storage_layout(&runtime_home).unwrap());
    assert!(store
        .runtime_storage_layout(&runtime_home)
        .unwrap()
        .is_none());
}

/// Counts all agent rows in the store.
fn agents(store: &Store) -> i64 {
    store
        .conn
        .query_row("SELECT COUNT(*) FROM agents", [], |row| row.get(0))
        .unwrap()
}
