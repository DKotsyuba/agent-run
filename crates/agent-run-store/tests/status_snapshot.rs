//! Synthetic status proof tests; no provider process or live service is started.

mod common;

use agent_run_domain::{
    Error, ProviderStartRequest,
    batch::{ExecutionRef, PinId},
    catalog::{HarnessId, ProviderConnection, ResolvedLaunchAuthority},
    domain::AgentId,
    status::{
        CleanupState, Coverage, PinState, RestartResume, StatusMatch, StatusRequest,
        StatusSelector, SuccessCommit,
    },
};
use agent_run_store::{Store, status::read_in_snapshot};
use rusqlite::params;
use serde_json::json;

/// Creates one fake v2 row and a valid first attempt in a disposable home.
fn execution(
    home: &common::Home,
    store: &mut Store<'_>,
    status: &str,
) -> (AgentId, String, ExecutionRef) {
    let request = home.request();
    let (id, _) = store
        .admit(&request, &home.config, &json!({}), None)
        .unwrap();
    let provider: ProviderStartRequest = serde_json::from_value(json!({"provider":"mock", "model":"fixture", "profile":"review", "task":request.task, "workdir":home.path, "explicit_finish":false})).unwrap();
    let mut role = json!({"role_name":"review"});
    role["config_revision"] = json!(agent_run_domain::canonical::sha256_hex(&role, true));
    let authority = ResolvedLaunchAuthority {
        provider: "mock".parse().unwrap(),
        harness: HarnessId::ClaudeCode,
        connection: ProviderConnection::Native,
        model: "fixture".into(),
        effort: None,
        profile: "review".into(),
        workdir: home.path.clone(),
        role_payload: role,
        assets_sha256: "0".repeat(64).parse().unwrap(),
        eligible_accounts: vec!["acct".parse().unwrap()],
    };
    let value = serde_json::to_value(&provider).unwrap();
    let identity = json!({"provider_identity_version":2,"provider_request":value,
        "replay_request_sha256":agent_run_domain::canonical::sha256_hex(&serde_json::to_value(&provider).unwrap(),true),
        "authority":authority});
    store
        .conn
        .execute(
            "UPDATE agents SET identity_json=?,status=?,finished_at=1 WHERE id=?",
            params![identity.to_string(), status, id.as_str()],
        )
        .unwrap();
    let attempt = store.create_attempt(&id, "failed", &json!({})).unwrap();
    store.conn.execute("UPDATE attempts SET selected_account_id='acct',phase='cleanup_complete',ownership_active=0,cleanup_proof_json=? WHERE id=?",
        params![json!({"confirmed":true,"group_gone":true,"descendants_gone":true}).to_string(),attempt]).unwrap();
    let reference = ExecutionRef::from_first_provider_attempt(&id, &attempt).unwrap();
    (id, attempt, reference)
}

fn pin(store: &Store<'_>, root: &AgentId) -> PinId {
    let pin = PinId::new();
    store
        .conn
        .execute(
            "INSERT INTO history_pins(pin_id,root_agent_id) VALUES(?,?)",
            params![pin.as_str(), root.as_str()],
        )
        .unwrap();
    pin
}

fn selector(
    id: &AgentId,
    sequence: u32,
    reference: Option<&ExecutionRef>,
    pin: Option<&PinId>,
) -> StatusSelector {
    StatusSelector {
        agent_id: id.clone(),
        sequence,
        execution_ref: reference.cloned(),
        pin_id: pin.cloned(),
    }
}

fn request(targets: Vec<StatusSelector>) -> StatusRequest {
    StatusRequest {
        version: 1,
        expected_store_id: None,
        targets,
        include_view: false,
    }
}

fn link(store: &Store<'_>, root: &AgentId, child: &AgentId) {
    store
        .conn
        .execute(
            "UPDATE agents SET root_agent_id=?,parent_agent_id=?,sequence=2 WHERE id=?",
            params![root.as_str(), root.as_str(), child.as_str()],
        )
        .unwrap();
}

/// Seeds trusted-commit-shaped facts without opening or creating an answer file.
fn terminal_evidence(
    home: &common::Home,
    store: &Store<'_>,
    id: &AgentId,
    attempt: &str,
    outcome: &str,
) -> String {
    let path = home.path.join("agents").join(id.as_str()).join("answer.md");
    let hash = "a".repeat(64);
    store.conn.execute("UPDATE agents SET status=?,finished_at=1,exit_code=0,failure_kind=NULL,failure_text=NULL,answer_path=?,answer_bytes=5,answer_sha256=? WHERE id=?",
        params![outcome,path.to_str().unwrap(),hash,id.as_str()]).unwrap();
    store
        .conn
        .execute(
            "UPDATE attempts SET state=?,finished_at=1 WHERE id=?",
            params![outcome, attempt],
        )
        .unwrap();
    store.conn.execute("INSERT INTO events(agent_id,attempt_id,at,kind,from_status,to_status,data_json) VALUES(?,?,1,'status','running',?,?)",
        params![id.as_str(),attempt,outcome,json!({"failure_kind":null}).to_string()]).unwrap();
    path.to_str().unwrap().to_owned()
}

/// Counts, database scope and canonical alias duplicates refuse before any result.
#[test]
fn status_validates_bounds_scope_and_duplicate_aliases() {
    let home = common::Home::new();
    let mut store = home.store();
    let (root, _, reference) = execution(&home, &mut store, "succeeded");
    let pin = pin(&store, &root);
    let (child, _, _) = execution(&home, &mut store, "succeeded");
    link(&store, &root, &child);
    let target = selector(&root, 1, Some(&reference), Some(&pin));
    assert!(store.status_snapshot(&request(vec![])).is_err());
    assert!(
        store
            .status_snapshot(&request(vec![target.clone(); 21]))
            .is_err()
    );
    let mut multi = request(vec![target.clone(), selector(&root, 2, None, Some(&pin))]);
    multi.include_view = true;
    assert!(store.status_snapshot(&multi).is_err());
    assert!(
        store
            .status_snapshot(&request(vec![selector(&root, 0, None, None)]))
            .is_err()
    );
    assert!(
        store
            .status_snapshot(&request(vec![
                target.clone(),
                selector(&child, 1, Some(&reference), Some(&pin))
            ]))
            .is_err()
    );
    let other = common::Home::new();
    let foreign = agent_run_store::store_identity::read(&other.store().conn).unwrap();
    let mut wrong = request(vec![target]);
    wrong.expected_store_id = Some(foreign);
    assert!(matches!(
        store.status_snapshot(&wrong),
        Err(Error::StoreIdentityMismatch)
    ));
    assert!(
        read_in_snapshot(&store.conn, &wrong).is_err(),
        "caller must own a read snapshot"
    );
}

/// Ordered exact results never turn absent or mismatched incarnations into the tail.
#[test]
fn status_preserves_order_missing_mismatch_and_unverified() {
    let home = common::Home::new();
    let mut store = home.store();
    let (first, _, first_ref) = execution(&home, &mut store, "succeeded");
    let first_pin = pin(&store, &first);
    let (second, _, second_ref) = execution(&home, &mut store, "failed");
    let second_pin = pin(&store, &second);
    let missing = AgentId::new();
    let result = store
        .status_snapshot(&request(vec![
            selector(&second, 1, Some(&second_ref), Some(&second_pin)),
            selector(&missing, 1, None, None),
            selector(&first, 1, Some(&second_ref), Some(&first_pin)),
            selector(&second, 2, None, Some(&second_pin)),
            selector(&first, 1, None, Some(&second_pin)),
        ]))
        .unwrap_err();
    assert!(
        matches!(result, Error::Validation(_)),
        "same canonical target is duplicate"
    );
    let result = store
        .status_snapshot(&request(vec![
            selector(&second, 1, Some(&second_ref), Some(&second_pin)),
            selector(&missing, 1, None, None),
            selector(&first, 1, Some(&second_ref), Some(&first_pin)),
            selector(&second, 2, None, Some(&second_pin)),
        ]))
        .unwrap();
    let items = &result.items;
    assert_eq!(
        items.iter().map(|v| v.r#match).collect::<Vec<_>>(),
        vec![
            StatusMatch::Verified,
            StatusMatch::Missing,
            StatusMatch::Mismatch,
            StatusMatch::Missing
        ]
    );
    assert_eq!(items[0].selector.agent_id, second);
    assert_eq!(items[0].coverage, Coverage::Complete);
    assert_eq!(items[0].lineage_terminal, Some(true));
    assert_eq!(items[0].cleanup.state, CleanupState::Confirmed);
    assert!(items[2].selected.is_none() && items[2].retained_latest.is_none());
    assert_eq!(items[2].coverage, Coverage::Unknown);
    let unverified = store
        .status_snapshot(&request(vec![selector(&first, 1, None, Some(&first_pin))]))
        .unwrap();
    assert_eq!(unverified.items[0].r#match, StatusMatch::Unverified);
    assert_eq!(unverified.items[0].lineage_terminal, None);
    let wrong_pin = store
        .status_snapshot(&request(vec![selector(
            &first,
            1,
            Some(&first_ref),
            Some(&second_pin),
        )]))
        .unwrap();
    assert_eq!(wrong_pin.items[0].pin_state, PinState::Mismatch);
    assert_eq!(wrong_pin.items[0].coverage, Coverage::Unknown);
}

/// Every attempt must have valid cleanup; the incarnation stays tied to attempt one.
#[test]
fn status_checks_all_attempts_and_strict_cleanup_proofs() {
    let home = common::Home::new();
    let mut store = home.store();
    let (root, first, reference) = execution(&home, &mut store, "failed");
    let pin = pin(&store, &root);
    let second = store.create_attempt(&root, "running", &json!({})).unwrap();
    store.conn.execute("UPDATE attempts SET selected_account_id='acct',phase='spawning',ownership_active=1 WHERE id=?",[&second]).unwrap();
    let req = request(vec![selector(&root, 1, Some(&reference), Some(&pin))]);
    let pending = store.status_snapshot(&req).unwrap();
    assert_eq!(
        pending.items[0]
            .selected
            .as_ref()
            .unwrap()
            .execution_ref
            .as_ref(),
        Some(&reference)
    );
    assert_eq!(pending.items[0].cleanup.state, CleanupState::Pending);
    store.conn.execute("UPDATE attempts SET ownership_active=0,state='failed',phase='cleanup_complete',cleanup_proof_json=? WHERE id=?",
        params![json!({"confirmed":true,"group_gone":true,"descendants_gone":true}).to_string(),second]).unwrap();
    assert_eq!(
        store.status_snapshot(&req).unwrap().items[0].cleanup.state,
        CleanupState::Confirmed
    );
    for proof in [
        json!({"confirmed":true,"group_gone":true}),
        json!({"confirmed":true,"group_gone":false,"descendants_gone":true}),
        json!({"confirmed":true,"group_gone":true,"descendants_gone":null}),
    ] {
        store
            .conn
            .execute(
                "UPDATE attempts SET cleanup_proof_json=? WHERE id=?",
                params![proof.to_string(), first],
            )
            .unwrap();
        let item = store.status_snapshot(&req).unwrap().items.remove(0);
        assert_eq!(item.cleanup.state, CleanupState::Unknown);
        assert_eq!(item.coverage, Coverage::Unknown);
        assert_eq!(item.lineage_terminal, None);
    }
    store.conn.execute("UPDATE attempts SET state='failed',cleanup_proof_json='{\"never_spawned\":true}',process_identity=NULL,process_birth_time=NULL WHERE id=?",[&first]).unwrap();
    assert_eq!(
        store.status_snapshot(&req).unwrap().items[0].cleanup.state,
        CleanupState::Confirmed
    );
    store
        .conn
        .execute(
            "UPDATE attempts SET process_identity='synthetic' WHERE id=?",
            [&first],
        )
        .unwrap();
    assert_eq!(
        store.status_snapshot(&req).unwrap().items[0].cleanup.state,
        CleanupState::Unknown
    );
    store
        .conn
        .execute(
            "UPDATE attempts SET process_identity=NULL,process_birth_time=1 WHERE id=?",
            [&first],
        )
        .unwrap();
    assert_eq!(
        store.status_snapshot(&req).unwrap().items[0].cleanup.state,
        CleanupState::Unknown
    );
    store
        .conn
        .execute(
            "UPDATE attempts SET process_birth_time=NULL WHERE id=?",
            [&first],
        )
        .unwrap();
    store.conn.execute("INSERT INTO process_ownership(owner_kind,owner_id,leader_json,descendants_observed,updated_at) VALUES('attempt',?,'{}',0,1)",[&first]).unwrap();
    assert_eq!(
        store.status_snapshot(&req).unwrap().items[0].cleanup.state,
        CleanupState::Unknown
    );
    store
        .conn
        .execute(
            "DELETE FROM process_ownership WHERE owner_kind='attempt' AND owner_id=?",
            [&first],
        )
        .unwrap();
    store
        .conn
        .execute(
            "UPDATE attempts SET cleanup_proof_json='not-json' WHERE id=?",
            [&first],
        )
        .unwrap();
    assert_eq!(
        store.status_snapshot(&req).unwrap().items[0].cleanup.state,
        CleanupState::Unknown
    );
    let identity: String = store
        .conn
        .query_row(
            "SELECT identity_json FROM agents WHERE id=?",
            [root.as_str()],
            |row| row.get(0),
        )
        .unwrap();
    store
        .conn
        .execute(
            "UPDATE agents SET identity_json='{}' WHERE id=?",
            [root.as_str()],
        )
        .unwrap();
    let legacy = store.status_snapshot(&req).unwrap();
    assert_eq!(legacy.items[0].r#match, StatusMatch::Unknown);
    assert_eq!(legacy.items[0].coverage, Coverage::Unknown);
    store
        .conn
        .execute(
            "UPDATE agents SET identity_json=? WHERE id=?",
            params![identity, root.as_str()],
        )
        .unwrap();
    store
        .conn
        .execute("DELETE FROM attempts WHERE agent_id=?", [root.as_str()])
        .unwrap();
    let empty = store.status_snapshot(&req).unwrap();
    assert_eq!(empty.items[0].r#match, StatusMatch::Unknown);
    assert_eq!(empty.items[0].cleanup.state, CleanupState::Unknown);
}

/// A released/missing pin cannot establish lineage terminality or cleanup.
#[test]
fn status_requires_active_pin_for_coverage() {
    let home = common::Home::new();
    let mut store = home.store();
    let (root, _, reference) = execution(&home, &mut store, "succeeded");
    let pin = pin(&store, &root);
    let req = request(vec![selector(&root, 1, Some(&reference), Some(&pin))]);
    assert_eq!(
        store.status_snapshot(&req).unwrap().items[0].coverage,
        Coverage::Complete
    );
    store.release_history_pin(&pin).unwrap().unwrap();
    let item = store.status_snapshot(&req).unwrap().items.remove(0);
    assert_eq!(item.pin_state, PinState::Released);
    assert_eq!(item.coverage, Coverage::Unknown);
    assert_eq!(item.lineage_terminal, None);
    assert_eq!(item.cleanup.state, CleanupState::Unknown);
    let no_pin = store
        .status_snapshot(&request(vec![selector(&root, 1, Some(&reference), None)]))
        .unwrap();
    assert_eq!(no_pin.items[0].pin_state, PinState::Missing);
    assert_eq!(no_pin.items[0].coverage, Coverage::Unknown);
    let nonexistent = PinId::new();
    let missing = store
        .status_snapshot(&request(vec![selector(
            &root,
            1,
            Some(&reference),
            Some(&nonexistent),
        )]))
        .unwrap();
    assert_eq!(missing.items[0].pin_state, PinState::Missing);
    assert_eq!(missing.items[0].coverage, Coverage::Unknown);
}

/// A running predecessor or a gap cannot establish a complete lineage.
#[test]
fn status_rejects_nonterminal_predecessor_and_broken_chain() {
    let home = common::Home::new();
    let mut store = home.store();
    let (root, _, reference) = execution(&home, &mut store, "running");
    let pin = pin(&store, &root);
    let (child, _, _) = execution(&home, &mut store, "succeeded");
    link(&store, &root, &child);
    let req = request(vec![selector(&root, 1, Some(&reference), Some(&pin))]);
    let item = store.status_snapshot(&req).unwrap().items.remove(0);
    assert_eq!(item.r#match, StatusMatch::Verified);
    assert_eq!(item.coverage, Coverage::Unknown);
    assert_eq!(item.lineage_terminal, None);
    assert_eq!(item.cleanup.state, CleanupState::Unknown);
    assert_eq!(item.success_commit, SuccessCommit::Unknown);
    store
        .conn
        .execute(
            "UPDATE agents SET status='succeeded' WHERE id=?",
            [root.as_str()],
        )
        .unwrap();
    store
        .conn
        .execute("UPDATE agents SET sequence=3 WHERE id=?", [child.as_str()])
        .unwrap();
    let item = store.status_snapshot(&req).unwrap().items.remove(0);
    assert_eq!(item.coverage, Coverage::Unknown);
    assert_eq!(item.lineage_terminal, None);
    assert_eq!(item.cleanup.state, CleanupState::Unknown);
}

/// A malformed root on an existing row is unknown, not evidence of absence.
#[test]
fn status_malformed_root_is_unknown_instead_of_missing() {
    let home = common::Home::new();
    let mut store = home.store();
    let (root, _, reference) = execution(&home, &mut store, "succeeded");
    let pin = pin(&store, &root);
    store
        .conn
        .execute(
            "UPDATE agents SET root_agent_id='not-an-agent' WHERE id=?",
            [root.as_str()],
        )
        .unwrap();
    let item = store
        .status_snapshot(&request(vec![selector(
            &root,
            1,
            Some(&reference),
            Some(&pin),
        )]))
        .unwrap()
        .items
        .remove(0);
    assert_eq!(item.r#match, StatusMatch::Unknown);
    assert!(item.canonical_agent_id.is_none());
    assert_eq!(item.coverage, Coverage::Unknown);
    assert_eq!(item.cleanup.state, CleanupState::Unknown);
}

/// Pinned status retains detailed evidence; neither identities nor a summary replace cleanup proof.
#[test]
fn status_pin_preserves_process_evidence_and_requires_attempt_proof() {
    let home = common::Home::new();
    let mut store = home.store();
    let (root, attempt, reference) = execution(&home, &mut store, "succeeded");
    let pin = pin(&store, &root);
    store.conn.execute("INSERT INTO process_ownership(owner_kind,owner_id,leader_json,descendants_observed,updated_at) VALUES('attempt',?,'{}',1,1)",[&attempt]).unwrap();
    store.conn.execute("INSERT INTO process_members(owner_kind,owner_id,pid,token,identity_json) VALUES('attempt',?,4242,'fixture-token',?)",params![attempt,json!({"program":"fixture"}).to_string()]).unwrap();
    let tx = agent_run_store::writer::WriteTx::begin(&store.conn).unwrap();
    assert!(
        !agent_run_store::process_ownership::compact_terminal_processes(&tx, root.as_str())
            .unwrap()
    );
    tx.commit().unwrap();
    let req = request(vec![selector(&root, 1, Some(&reference), Some(&pin))]);
    assert_eq!(
        store.status_snapshot(&req).unwrap().items[0].cleanup.state,
        CleanupState::Confirmed
    );
    let summaries: i64 = store
        .conn
        .query_row(
            "SELECT COUNT(*) FROM events WHERE agent_id=? AND kind='process_summary_v1'",
            [root.as_str()],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(summaries, 0);
    let members: i64 = store
        .conn
        .query_row(
            "SELECT COUNT(*) FROM process_members WHERE owner_kind='attempt' AND owner_id=?",
            [&attempt],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        members, 1,
        "the active pin must retain detailed process evidence"
    );
    // Even an aggregate summary cannot fill in a missing per-attempt proof.
    store
        .conn
        .execute(
            "INSERT INTO events(agent_id,at,kind,data_json) VALUES(?,1,'process_summary_v1',?)",
            params![root.as_str(), json!({"cleanup_confirmed":true}).to_string()],
        )
        .unwrap();
    store
        .conn
        .execute(
            "UPDATE attempts SET cleanup_proof_json=NULL WHERE id=?",
            [&attempt],
        )
        .unwrap();
    let item = store.status_snapshot(&req).unwrap().items.remove(0);
    assert_eq!(item.coverage, Coverage::Unknown);
    assert_eq!(item.cleanup.state, CleanupState::Unknown);
}

/// A historical success requires a coherent terminal event and answer metadata only.
#[test]
fn status_success_commit_requires_nontechnical_event_and_answer_metadata() {
    let home = common::Home::new();
    let mut store = home.store();
    let (root, attempt, reference) = execution(&home, &mut store, "succeeded");
    let success_pin = pin(&store, &root);
    let req = request(vec![selector(
        &root,
        1,
        Some(&reference),
        Some(&success_pin),
    )]);
    assert_eq!(
        store.status_snapshot(&req).unwrap().items[0].success_commit,
        SuccessCommit::Unknown
    );
    let path = terminal_evidence(&home, &store, &root, &attempt, "succeeded");
    let recorded = store.status_snapshot(&req).unwrap();
    assert_eq!(recorded.items[0].success_commit, SuccessCommit::Recorded);
    let serialized = serde_json::to_string(&recorded).unwrap();
    assert!(!serialized.contains(&path));
    assert!(!serialized.contains("answer.md"));
    let nul_path = format!("/bad\0prefix/agents/{}/answer.md", root.as_str());
    store
        .conn
        .execute(
            "UPDATE agents SET answer_path=? WHERE id=?",
            params![nul_path, root.as_str()],
        )
        .unwrap();
    assert_eq!(
        store.status_snapshot(&req).unwrap().items[0].success_commit,
        SuccessCommit::Unknown
    );
    store
        .conn
        .execute(
            "UPDATE agents SET answer_path=? WHERE id=?",
            params![path, root.as_str()],
        )
        .unwrap();
    assert_eq!(
        store.status_snapshot(&req).unwrap().items[0].success_commit,
        SuccessCommit::Recorded
    );
    for bytes in [0, -1, 16 * 1024 * 1024 + 1] {
        store
            .conn
            .execute(
                "UPDATE agents SET answer_bytes=? WHERE id=?",
                params![bytes, root.as_str()],
            )
            .unwrap();
        assert_eq!(
            store.status_snapshot(&req).unwrap().items[0].success_commit,
            SuccessCommit::Unknown
        );
    }
    store
        .conn
        .execute(
            "UPDATE agents SET answer_bytes=5,answer_sha256='bad' WHERE id=?",
            [root.as_str()],
        )
        .unwrap();
    assert_eq!(
        store.status_snapshot(&req).unwrap().items[0].success_commit,
        SuccessCommit::Unknown
    );
    store
        .conn
        .execute(
            "UPDATE agents SET answer_sha256=?,answer_path=NULL WHERE id=?",
            params!["a".repeat(64), root.as_str()],
        )
        .unwrap();
    assert_eq!(
        store.status_snapshot(&req).unwrap().items[0].success_commit,
        SuccessCommit::Unknown
    );
    store
        .conn
        .execute(
            "UPDATE agents SET answer_path=? WHERE id=?",
            params![path, root.as_str()],
        )
        .unwrap();
    store
        .conn
        .execute(
            "UPDATE events SET data_json=? WHERE agent_id=? AND kind='status'",
            params![
                json!({"failure_kind":null,"service_restart":true}).to_string(),
                root.as_str()
            ],
        )
        .unwrap();
    assert_eq!(
        store.status_snapshot(&req).unwrap().items[0].success_commit,
        SuccessCommit::Unknown
    );
    store
        .conn
        .execute(
            "UPDATE events SET data_json=? WHERE agent_id=? AND kind='status'",
            params![json!({"failure_kind":null}).to_string(), root.as_str()],
        )
        .unwrap();
    store
        .conn
        .execute("UPDATE attempts SET state='failed' WHERE id=?", [&attempt])
        .unwrap();
    assert_eq!(
        store.status_snapshot(&req).unwrap().items[0].success_commit,
        SuccessCommit::Unknown
    );
    store
        .conn
        .execute(
            "UPDATE attempts SET state='succeeded' WHERE id=?",
            [&attempt],
        )
        .unwrap();
    store
        .conn
        .execute(
            "UPDATE events SET to_status='failed' WHERE agent_id=? AND kind='status'",
            [root.as_str()],
        )
        .unwrap();
    assert_eq!(
        store.status_snapshot(&req).unwrap().items[0].success_commit,
        SuccessCommit::Unknown
    );

    let (failed, failed_attempt, failed_ref) = execution(&home, &mut store, "failed");
    let failed_pin = pin(&store, &failed);
    let failed_req = request(vec![selector(
        &failed,
        1,
        Some(&failed_ref),
        Some(&failed_pin),
    )]);
    assert_eq!(
        store.status_snapshot(&failed_req).unwrap().items[0].success_commit,
        SuccessCommit::Unknown
    );
    terminal_evidence(&home, &store, &failed, &failed_attempt, "failed");
    assert_eq!(
        store.status_snapshot(&failed_req).unwrap().items[0].success_commit,
        SuccessCommit::NotSucceeded
    );
}

/// Malformed SQLite exit types cannot turn into the accepted NULL success case or acquire a hold.
#[test]
fn status_success_and_hold_require_sql_null_or_integer_zero_exit() {
    use agent_run_domain::lineage_hold::{LineageHoldAcquire, LineageHoldTarget};
    use rusqlite::types::Value;

    for (exit, storage_type, accepted) in [
        (Value::Text("not-an-exit-code".into()), "text", false),
        (Value::Blob(vec![b'0']), "blob", false),
        (Value::Real(0.5), "real", false),
        (Value::Integer(1), "integer", false),
        (Value::Integer(-1), "integer", false),
        (Value::Null, "null", true),
        (Value::Integer(0), "integer", true),
    ] {
        let home = common::Home::new();
        let mut store = home.store();
        let (root, attempt, reference) = execution(&home, &mut store, "succeeded");
        terminal_evidence(&home, &store, &root, &attempt, "succeeded");
        let pin = pin(&store, &root);
        store
            .conn
            .execute(
                "UPDATE agents SET exit_code=? WHERE id=?",
                params![exit, root.as_str()],
            )
            .unwrap();
        let actual_type: String = store
            .conn
            .query_row(
                "SELECT typeof(exit_code) FROM agents WHERE id=?",
                [root.as_str()],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            actual_type, storage_type,
            "SQLite affinity changed the fixture"
        );
        let status = store
            .status_snapshot(&request(vec![selector(
                &root,
                1,
                Some(&reference),
                Some(&pin),
            )]))
            .unwrap();
        assert_eq!(status.items[0].coverage, Coverage::Complete);
        assert_eq!(
            status.items[0].success_commit,
            if accepted {
                SuccessCommit::Recorded
            } else {
                SuccessCommit::Unknown
            },
            "stored exit: {exit:?}"
        );
        let hold = LineageHoldAcquire {
            version: 1,
            store_id: status.store_id,
            request_id: "typed-exit".into(),
            targets: vec![LineageHoldTarget {
                agent_id: root,
                sequence: 1,
                execution_ref: reference,
                pin_id: pin,
            }],
            verifying_key: ed25519_dalek::SigningKey::from_bytes(&[7; 32])
                .verifying_key()
                .to_bytes(),
            intent_digest: None,
        };
        let acquired = store.acquire_lineage_hold(&hold);
        if accepted {
            assert!(
                acquired.is_ok(),
                "stored exit: {exit:?}, result: {acquired:?}"
            );
        } else {
            assert!(
                matches!(acquired, Err(Error::Conflict)),
                "stored exit: {exit:?}, result: {acquired:?}"
            );
        }
        let held: i64 = store
            .conn
            .query_row("SELECT count(*) FROM lineage_holds", [], |row| row.get(0))
            .unwrap();
        assert_eq!(held, i64::from(accepted));
    }
}

/// Restart pending is nonterminal; an unconfirmed continuation is unknown.
#[test]
fn status_never_marks_restart_pending_or_unconfirmed_terminal() {
    let home = common::Home::new();
    let mut store = home.store();
    let (root, _, reference) = execution(&home, &mut store, "succeeded");
    let pin = pin(&store, &root);
    store.conn.execute("INSERT INTO service_restarts(request_id,phase,started_at,boot_id) VALUES('fixture','preparing',1,'fixture')",[]).unwrap();
    let restart = store.conn.last_insert_rowid();
    store.conn.execute("INSERT INTO service_restart_agents(restart_id,agent_id,cancelled_before_restart,resume_status) VALUES(?,?,0,'pending')",params![restart,root.as_str()]).unwrap();
    let req = request(vec![selector(&root, 1, Some(&reference), Some(&pin))]);
    let item = store.status_snapshot(&req).unwrap().items.remove(0);
    assert_eq!(item.restart_resume, RestartResume::Pending);
    assert_eq!(item.lineage_terminal, Some(false));
    assert_eq!(item.cleanup.state, CleanupState::Pending);
    store
        .conn
        .execute(
            "UPDATE service_restart_agents SET resume_status='unconfirmed' WHERE restart_id=?",
            [restart],
        )
        .unwrap();
    store
        .conn
        .execute(
            "UPDATE service_restarts SET phase='complete',finished_at=2 WHERE id=?",
            [restart],
        )
        .unwrap();
    let item = store.status_snapshot(&req).unwrap().items.remove(0);
    assert_eq!(item.restart_resume, RestartResume::Unknown);
    assert_eq!(item.lineage_terminal, None);
    assert_eq!(item.cleanup.state, CleanupState::Unknown);
}

/// A second connection can add a resume while the first snapshot stays stable.
#[test]
fn status_and_future_view_share_one_read_snapshot() {
    let home = common::Home::new();
    let mut reader = home.store();
    let (root, _, reference) = execution(&home, &mut reader, "succeeded");
    let pin = pin(&reader, &root);
    let req = request(vec![selector(&root, 1, Some(&reference), Some(&pin))]);
    let tx = reader.conn.unchecked_transaction().unwrap();
    let before = read_in_snapshot(&tx, &req).unwrap();
    assert_eq!(
        before.response.items[0]
            .retained_latest
            .as_ref()
            .unwrap()
            .sequence,
        1
    );
    let mut writer = home.store();
    let (child, _, _) = execution(&home, &mut writer, "running");
    link(&writer, &root, &child);
    let same = read_in_snapshot(&tx, &req).unwrap();
    assert_eq!(
        same.response.items[0]
            .retained_latest
            .as_ref()
            .unwrap()
            .sequence,
        1
    );
    assert_eq!(same.retained_latest_execution(0), Some(&root));
    drop(same);
    drop(before);
    tx.commit().unwrap();
    let after = reader.status_snapshot(&req).unwrap();
    assert_eq!(after.items[0].retained_latest.as_ref().unwrap().sequence, 2);
    assert_eq!(after.items[0].retained_latest.as_ref().unwrap().sequence, 2);
    assert_eq!(after.items[0].lineage_terminal, Some(false));
}

/// Builds a strict export request using the same real proof fixture as status.
fn exact_request(
    store: &Store<'_>,
    id: &AgentId,
    reference: &ExecutionRef,
    pin: &PinId,
) -> agent_run_domain::transcript_exact::TranscriptExactRequest {
    agent_run_domain::transcript_exact::TranscriptExactRequest {
        version: 1,
        expected_store_id: agent_run_store::store_identity::read(&store.conn).unwrap(),
        selector: agent_run_domain::transcript_exact::ExactSelector {
            agent_id: id.clone(),
            sequence: 1,
            execution_ref: reference.clone(),
            pin_id: pin.clone(),
        },
        cursor: "0".into(),
        upper_seq: None,
        offset: 0,
        expected_sha256: None,
    }
}

/// Adds immutable-looking inline fixture rows without invoking a worker or spool.
fn exact_message(store: &Store<'_>, id: &AgentId, seq: i64, body: &str, complete: Option<bool>) {
    store.conn.execute("INSERT INTO messages(seq,agent_id,root_agent_id,at,role,content,content_complete) VALUES(?1,?2,?2,1,'assistant',?3,?4)", params![seq,id.as_str(),body,complete]).unwrap();
}

/// Advances exactly as a caller does; no numeric sequence conversion is involved.
fn exact_continue(
    request: &mut agent_run_domain::transcript_exact::TranscriptExactRequest,
    page: &agent_run_domain::transcript_exact::TranscriptExactResponse,
) {
    request.upper_seq = Some(page.upper_seq.clone());
    request.cursor = page.next_cursor.clone();
    request.offset = page.next_offset;
    request.expected_sha256 =
        (page.next_offset != 0).then(|| page.message.as_ref().unwrap().sha256.clone());
}

/// Every wrong store/incarnation/pin/missing row fails without returning content.
#[test]
fn transcript_exact_fences_identity_and_pin_release() {
    use agent_run_store::transcript_exact::read;
    let home = common::Home::new();
    let mut store = home.store();
    let (id, _, reference) = execution(&home, &mut store, "succeeded");
    let pin = pin(&store, &id);
    exact_message(&store, &id, 1, "private fixture body", Some(true));
    let request = exact_request(&store, &id, &reference, &pin);
    assert!(read(&store.conn, &request).unwrap().snapshot_complete);
    let mut wrong = request.clone();
    wrong.expected_store_id = "store_00000000000000000000000000000000".parse().unwrap();
    assert!(matches!(
        read(&store.conn, &wrong),
        Err(Error::StoreIdentityMismatch)
    ));
    let (other, _, other_ref) = execution(&home, &mut store, "failed");
    for field in 0..4 {
        let mut wrong = request.clone();
        match field {
            0 => wrong.selector.execution_ref = other_ref.clone(),
            1 => wrong.selector.pin_id = PinId::new(),
            2 => wrong.selector.agent_id = other.clone(),
            _ => wrong.selector.sequence = 2,
        };
        assert!(matches!(
            read(&store.conn, &wrong),
            Err(Error::TranscriptExportAborted(_))
        ));
    }
    store
        .conn
        .execute(
            "UPDATE history_pins SET released=1 WHERE pin_id=?",
            [pin.as_str()],
        )
        .unwrap();
    assert!(matches!(
        read(&store.conn, &request),
        Err(Error::TranscriptExportAborted(_))
    ));
}

/// Lossless UTF-8 fragments retain >2^53 sequence values and a fixed execution watermark.
#[test]
fn transcript_exact_chunks_utf8_and_freezes_upper_before_append_and_resume() {
    use agent_run_store::transcript_exact::read;
    let home = common::Home::new();
    let mut store = home.store();
    let (id, _, reference) = execution(&home, &mut store, "succeeded");
    let pin = pin(&store, &id);
    let body = "€🐈\\\"\n".repeat(12_000);
    let first_seq = 9_007_199_254_740_993i64;
    exact_message(&store, &id, first_seq, &body, Some(true));
    exact_message(&store, &id, first_seq + 1, "last original row", Some(true));
    let mut request = exact_request(&store, &id, &reference, &pin);
    let first = read(&store.conn, &request).unwrap();
    assert_eq!(first.upper_seq, (first_seq + 1).to_string());
    assert!(!first.snapshot_complete);
    let hash = first.message.as_ref().unwrap().sha256.clone();
    let total = first.message.as_ref().unwrap().total_bytes;
    let mut bytes = first.message.as_ref().unwrap().text.clone();
    let mut pages = 1;
    exact_continue(&mut request, &first);
    exact_message(
        &store,
        &id,
        first_seq + 2,
        "appended after capture",
        Some(true),
    );
    let (child, _, _) = execution(&home, &mut store, "running");
    link(&store, &id, &child);
    exact_message(
        &store,
        &child,
        first_seq + 3,
        "new execution must not leak",
        Some(true),
    );
    loop {
        let page = read(&store.conn, &request).unwrap();
        let chunk = page.message.as_ref().unwrap();
        pages += 1;
        assert_eq!(chunk.seq, first_seq.to_string());
        assert_eq!(chunk.sha256, hash);
        assert!(chunk.text.len() <= 32 * 1024);
        bytes.push_str(&chunk.text);
        exact_continue(&mut request, &page);
        if chunk.complete {
            break;
        }
    }
    assert!(pages > 2);
    assert_eq!(bytes.len(), total);
    assert_eq!(
        agent_run_platform::fs::sha256(bytes.as_bytes()),
        hash.as_str()
    );
    let message: serde_json::Value = serde_json::from_str(&bytes).unwrap();
    assert_eq!(message["content"], body);
    assert_eq!(message["seq"], first_seq.to_string());
    let last = read(&store.conn, &request).unwrap();
    assert!(last.snapshot_complete);
    assert_eq!(
        last.message.as_ref().unwrap().seq,
        (first_seq + 1).to_string()
    );
    exact_continue(&mut request, &last);
    let end = read(&store.conn, &request).unwrap();
    assert!(end.snapshot_complete);
    assert!(end.message.is_none());
}

/// Unknown/incomplete content never becomes a stub or a completed export.
#[test]
fn transcript_exact_aborts_on_false_unknown_and_missing_continuation() {
    use agent_run_store::transcript_exact::read;
    for incomplete in [Some(false), None] {
        let home = common::Home::new();
        let mut store = home.store();
        let (id, _, reference) = execution(&home, &mut store, "succeeded");
        let pin = pin(&store, &id);
        exact_message(&store, &id, 1, "fully stored", Some(true));
        exact_message(&store, &id, 2, "spool:do-not-open", incomplete);
        let mut request = exact_request(&store, &id, &reference, &pin);
        let page = read(&store.conn, &request).unwrap();
        assert!(!page.snapshot_complete);
        exact_continue(&mut request, &page);
        assert!(matches!(
            read(&store.conn, &request),
            Err(Error::TranscriptExportAborted(_))
        ));
        store
            .conn
            .execute("DELETE FROM messages WHERE seq=2", [])
            .unwrap();
        assert!(matches!(
            read(&store.conn, &request),
            Err(Error::TranscriptExportAborted(_))
        ));
    }
}

/// Missing lineage members or attempts cannot be mistaken for a complete export.
#[test]
fn transcript_exact_requires_complete_lineage() {
    use agent_run_store::transcript_exact::read;
    let home = common::Home::new();
    let mut store = home.store();
    let (id, _, reference) = execution(&home, &mut store, "succeeded");
    let pin = pin(&store, &id);
    let (child, _, child_ref) = execution(&home, &mut store, "succeeded");
    link(&store, &id, &child);
    let mut request = exact_request(&store, &id, &child_ref, &pin);
    request.selector.sequence = 2;
    exact_message(&store, &child, 1, "retained child", Some(true));
    assert!(read(&store.conn, &request).is_ok());
    store
        .conn
        .execute("UPDATE agents SET sequence=3 WHERE id=?", [child.as_str()])
        .unwrap();
    request.selector.sequence = 3;
    assert!(matches!(
        read(&store.conn, &request),
        Err(Error::TranscriptExportAborted(_))
    ));
    request.selector.sequence = 1;
    request.selector.execution_ref = reference;
    assert!(matches!(
        read(&store.conn, &request),
        Err(Error::TranscriptExportAborted(_))
    ));
}

/// Size limits and continuation hashes refuse before leaking partial oversized data.
#[test]
fn transcript_exact_rejects_oversize_metadata_encoding_and_digest_changes() {
    use agent_run_store::transcript_exact::read;
    let home = common::Home::new();
    let mut store = home.store();
    let (id, _, reference) = execution(&home, &mut store, "succeeded");
    let pin = pin(&store, &id);
    exact_message(&store, &id, 1, &"x".repeat(70_000), Some(true));
    let mut request = exact_request(&store, &id, &reference, &pin);
    let page = read(&store.conn, &request).unwrap();
    exact_continue(&mut request, &page);
    request.expected_sha256 = Some("0".repeat(64).parse().unwrap());
    assert!(matches!(
        read(&store.conn, &request),
        Err(Error::TranscriptExportAborted(_))
    ));
    request = exact_request(&store, &id, &reference, &pin);
    store
        .conn
        .execute("UPDATE messages SET name=? WHERE seq=1", ["x".repeat(4097)])
        .unwrap();
    assert!(matches!(
        read(&store.conn, &request),
        Err(Error::TranscriptExportAborted(_))
    ));
    store
        .conn
        .execute(
            "UPDATE messages SET name=NULL,content=? WHERE seq=1",
            ["\u{1}".repeat(3 * 1024 * 1024)],
        )
        .unwrap();
    assert!(matches!(
        read(&store.conn, &request),
        Err(Error::TranscriptExportAborted(_))
    ));
    store
        .conn
        .execute(
            "UPDATE messages SET content=? WHERE seq=1",
            ["x".repeat(16 * 1024 * 1024 + 1)],
        )
        .unwrap();
    assert!(matches!(
        read(&store.conn, &request),
        Err(Error::TranscriptExportAborted(_))
    ));
}

/// An active pin retains transcript rows; a release affects the next snapshot.
#[test]
fn transcript_exact_snapshot_and_retention_share_pin_fence() {
    use agent_run_store::transcript_exact::{read, read_in_snapshot};
    let home = common::Home::new();
    let mut store = home.store();
    let (id, _, reference) = execution(&home, &mut store, "succeeded");
    let pin = pin(&store, &id);
    exact_message(&store, &id, 1, "retained", Some(true));
    let request = exact_request(&store, &id, &reference, &pin);
    assert_eq!(store.prune_history(1e12).unwrap(), 0);
    assert!(read(&store.conn, &request).is_ok());
    let tx = store.conn.unchecked_transaction().unwrap();
    let page = read_in_snapshot(&tx, &request).unwrap();
    let other = home.store();
    other
        .conn
        .execute(
            "UPDATE history_pins SET released=1 WHERE pin_id=?",
            [pin.as_str()],
        )
        .unwrap();
    exact_message(&other, &id, 2, "later", Some(true));
    assert_eq!(read_in_snapshot(&tx, &request).unwrap(), page);
    tx.commit().unwrap();
    assert!(matches!(
        read(&store.conn, &request),
        Err(Error::TranscriptExportAborted(_))
    ));
}

/// Empty selected executions are explicit and a captured zero never includes later rows.
#[test]
fn transcript_exact_empty_capture_stays_empty() {
    use agent_run_store::transcript_exact::read;
    let home = common::Home::new();
    let mut store = home.store();
    let (id, _, reference) = execution(&home, &mut store, "running");
    let pin = pin(&store, &id);
    let mut request = exact_request(&store, &id, &reference, &pin);
    let page = read(&store.conn, &request).unwrap();
    assert_eq!(page.upper_seq, "0");
    assert!(page.message.is_none());
    assert!(page.snapshot_complete);
    exact_continue(&mut request, &page);
    exact_message(&store, &id, 1, "future", Some(true));
    assert_eq!(read(&store.conn, &request).unwrap(), page);
}
