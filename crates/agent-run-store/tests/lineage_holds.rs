//! Atomic success holds, permanent replay and authenticated release in disposable stores.

use crate::common;

#[path = "lineage_holds/writer.rs"]
mod writer;

use agent_run_domain::{
    Error, ProviderStartRequest,
    batch::{ExecutionRef, PinId},
    catalog::{HarnessId, ProviderConnection, ResolvedLaunchAuthority},
    domain::AgentId,
    lineage_hold::{LineageHoldAcquire, LineageHoldTarget, release_signed_bytes},
};
use agent_run_store::{
    Store,
    lineage_holds::{LineageHoldAcquireResult, LineageHoldFilter, LineageHoldState},
};
use ed25519_dalek::{Signer, SigningKey};
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

/// Inserts a synthetic initial retention pin for the fixture lineage.
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

/// Returns a complete success proof and its initial pin without spawning processes.
fn target(home: &common::Home, store: &mut Store<'_>) -> LineageHoldTarget {
    let (id, attempt, reference) = execution(home, store, "succeeded");
    terminal_evidence(home, store, &id, &attempt, "succeeded");
    LineageHoldTarget {
        pin_id: pin(store, &id),
        agent_id: id,
        sequence: 1,
        execution_ref: reference,
    }
}

/// Test-only deterministic signing key; never used as an operational credential.
fn key() -> SigningKey {
    SigningKey::from_bytes(&[7; 32])
}

/// Builds one structurally valid acquisition scoped to this disposable store.
fn acquire(
    store: &Store<'_>,
    request_id: &str,
    targets: Vec<LineageHoldTarget>,
) -> LineageHoldAcquire {
    LineageHoldAcquire {
        version: 1,
        store_id: agent_run_store::store_identity::read(&store.conn).unwrap(),
        request_id: request_id.into(),
        targets,
        verifying_key: key().verifying_key().to_bytes(),
        intent_digest: None,
    }
}

/// Signs only the exact durable receipt fields used by the release verifier.
fn signature(result: &LineageHoldAcquireResult) -> [u8; 64] {
    let receipt = &result.receipt;
    key()
        .sign(
            &release_signed_bytes(&receipt.store_id, &receipt.hold_id, &receipt.fingerprint)
                .unwrap(),
        )
        .to_bytes()
}

/// Counts persisted hold headers so failed multi-target acquisitions cannot leave partial protection.
fn count(store: &Store<'_>) -> i64 {
    store
        .conn
        .query_row("SELECT count(*) FROM lineage_holds", [], |row| row.get(0))
        .unwrap()
}

/// Active orphan growth is bounded without preventing exact replay or signed release.
#[test]
fn active_hold_capacity_preserves_replay_and_recovery() {
    use agent_run_domain::lineage_hold::HoldId;
    use agent_run_store::lineage_holds::MAX_ACTIVE_LINEAGE_HOLDS;

    let home = common::Home::new();
    let mut store = home.store();
    let target = target(&home, &mut store);
    let request = acquire(&store, "capacity-original", vec![target.clone()]);
    let original = store.acquire_lineage_hold(&request).unwrap();
    store.conn.execute(
        "WITH RECURSIVE n(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM n WHERE i<?1) \
         INSERT INTO lineage_holds(hold_id,request_id,version,fingerprint,verifying_key,target_count) \
         SELECT 'hold_000000000000400080000000'||printf('%08x',i),'capacity-'||i,1,zeroblob(32),?2,1 FROM n",
        params![MAX_ACTIVE_LINEAGE_HOLDS - 1, key().verifying_key().to_bytes().as_slice()],
    ).unwrap();
    store.conn.execute(
        "INSERT INTO lineage_hold_targets(hold_id,ordinal,agent_id,sequence,execution_ref,pin_id,root_agent_id) \
         SELECT hold_id,0,?1,?2,?3,?4,?1 FROM lineage_holds WHERE request_id LIKE 'capacity-%' AND request_id!='capacity-original'",
        params![target.agent_id.as_str(),target.sequence,target.execution_ref.as_str(),target.pin_id.as_str()],
    ).unwrap();
    let refused = acquire(&store, "capacity-next", vec![target]);
    assert!(store.acquire_lineage_hold(&refused).is_err());
    assert_eq!(count(&store) as usize, MAX_ACTIVE_LINEAGE_HOLDS);
    let replay = store.acquire_lineage_hold(&request).unwrap();
    assert!(replay.replayed);
    assert_eq!(replay.receipt, original.receipt);

    let held: HoldId = "hold_00000000000040008000000000000001".parse().unwrap();
    let message = release_signed_bytes(&request.store_id, &held, &[0; 32]).unwrap();
    let signature = key().sign(&message).to_bytes();
    store
        .release_lineage_hold(&held, &request.store_id, &signature)
        .unwrap()
        .unwrap();
    assert!(!store.acquire_lineage_hold(&refused).unwrap().replayed);
}

/// A durable receipt survives reopening and every replay preserves its exact ordered targets.
#[test]
fn acquire_reopen_replay_and_fingerprint_conflict() {
    let home = common::Home::new();
    let mut store = home.store();
    let first = target(&home, &mut store);
    let second = target(&home, &mut store);
    let request = acquire(&store, "ordered", vec![second.clone(), first.clone()]);
    let result = store.acquire_lineage_hold(&request).unwrap();
    assert!(!result.replayed);
    assert_eq!(result.state, LineageHoldState::Active);
    assert_eq!(result.receipt.targets, request.targets);
    assert_eq!(
        result.receipt.canonical_roots,
        vec![second.agent_id, first.agent_id]
    );
    drop(store);
    let mut store = home.store();
    let replay = store.acquire_lineage_hold(&request).unwrap();
    assert!(replay.replayed);
    assert_eq!(replay.receipt, result.receipt);
    let mut changed = request.clone();
    changed.targets.reverse();
    assert!(matches!(
        store.acquire_lineage_hold(&changed),
        Err(Error::Conflict)
    ));
    changed = request;
    changed.intent_digest = Some([0; 32]);
    assert!(matches!(
        store.acquire_lineage_hold(&changed),
        Err(Error::Conflict)
    ));
    assert_eq!(count(&store), 1);
}

/// One invalid target rolls back the whole acquisition, including previously valid roots.
#[test]
fn all_preconditions_fail_closed_and_atomic() {
    for mutation in 0..10 {
        let home = common::Home::new();
        let mut store = home.store();
        let first = target(&home, &mut store);
        let mut second = target(&home, &mut store);
        match mutation {
            0 => {
                store.release_history_pin(&second.pin_id).unwrap();
            }
            1 => second.execution_ref = format!("ex1_{}", "f".repeat(64)).parse().unwrap(),
            2 => second.sequence = 2,
            3 => {
                store
                    .conn
                    .execute(
                        "UPDATE agents SET answer_sha256=NULL WHERE id=?",
                        [second.agent_id.as_str()],
                    )
                    .unwrap();
            }
            4 => {
                store
                    .conn
                    .execute(
                        "UPDATE attempts SET cleanup_proof_json='{}' WHERE agent_id=?",
                        [second.agent_id.as_str()],
                    )
                    .unwrap();
            }
            5 => {
                store
                    .conn
                    .execute(
                        "UPDATE attempts SET ownership_active=1 WHERE agent_id=?",
                        [second.agent_id.as_str()],
                    )
                    .unwrap();
            }
            6 => {
                store
                    .conn
                    .execute(
                        "UPDATE agents SET status='failed' WHERE id=?",
                        [second.agent_id.as_str()],
                    )
                    .unwrap();
            }
            7 => {
                store
                    .conn
                    .execute(
                        "DELETE FROM events WHERE agent_id=? AND kind='status'",
                        [second.agent_id.as_str()],
                    )
                    .unwrap();
            }
            8 => {
                store.conn.execute("INSERT INTO service_restarts(request_id,phase,started_at,boot_id) VALUES('pending','preparing',1,'fixture')", []).unwrap();
                let restart = store.conn.last_insert_rowid();
                store
                    .conn
                    .execute(
                        "INSERT INTO service_restart_agents(restart_id,agent_id) VALUES(?,?)",
                        params![restart, second.agent_id.as_str()],
                    )
                    .unwrap();
            }
            _ => second.pin_id = first.pin_id.clone(),
        }
        let request = acquire(&store, "atomic", vec![first.clone(), second]);
        assert!(
            store.acquire_lineage_hold(&request).is_err(),
            "case {mutation}"
        );
        assert_eq!(count(&store), 0);
        assert!(
            store
                .create_attempt(&first.agent_id, "prepared", &json!({}))
                .is_ok()
        );
    }
}

/// A selected ancestor can never be held as the latest, even when the full lineage succeeded.
#[test]
fn stale_tail_and_duplicate_canonical_root_are_rejected() {
    let home = common::Home::new();
    let mut store = home.store();
    let root = target(&home, &mut store);
    let mut child = target(&home, &mut store);
    store
        .conn
        .execute(
            "UPDATE agents SET root_agent_id=?,parent_agent_id=?,sequence=2 WHERE id=?",
            params![
                root.agent_id.as_str(),
                root.agent_id.as_str(),
                child.agent_id.as_str()
            ],
        )
        .unwrap();
    child.sequence = 2;
    child.pin_id = root.pin_id.clone();
    let stale = acquire(&store, "stale", vec![root.clone()]);
    assert!(store.acquire_lineage_hold(&stale).is_err());
    let mut alias = child.clone();
    alias.agent_id = root.agent_id.clone();
    let duplicate = acquire(&store, "aliases", vec![alias, child.clone()]);
    assert!(store.acquire_lineage_hold(&duplicate).is_err());
    assert_eq!(count(&store), 0);
    let valid = acquire(&store, "tail", vec![child]);
    assert_eq!(
        store
            .acquire_lineage_hold(&valid)
            .unwrap()
            .receipt
            .canonical_roots,
        vec![root.agent_id]
    );
}

/// Bad signatures cannot release active or already released tombstones, including cross-hold replay.
#[test]
fn release_checks_signature_store_and_persisted_fingerprint_every_time() {
    let home = common::Home::new();
    let mut store = home.store();
    let target = target(&home, &mut store);
    let request = acquire(&store, "first", vec![target.clone()]);
    let first = store.acquire_lineage_hold(&request).unwrap();
    let second_request = acquire(&store, "second", vec![target]);
    let second = store.acquire_lineage_hold(&second_request).unwrap();
    let sig = signature(&first);
    let id = &first.receipt.hold_id;
    assert!(
        store
            .release_lineage_hold(id, &request.store_id, &[0; 64])
            .is_err()
    );
    let foreign = "store_00000000000000000000000000000001".parse().unwrap();
    assert!(matches!(
        store.release_lineage_hold(id, &foreign, &sig),
        Err(Error::StoreIdentityMismatch)
    ));
    assert!(
        store
            .release_lineage_hold(&second.receipt.hold_id, &request.store_id, &sig)
            .is_err()
    );
    let result = store
        .release_lineage_hold(id, &request.store_id, &sig)
        .unwrap()
        .unwrap();
    assert!(!result.already_released);
    assert!(
        store
            .release_lineage_hold(id, &request.store_id, &[0; 64])
            .is_err()
    );
    assert!(
        store
            .release_lineage_hold(id, &request.store_id, &sig)
            .unwrap()
            .unwrap()
            .already_released
    );
    let replay = store.acquire_lineage_hold(&request).unwrap();
    assert_eq!(replay.state, LineageHoldState::Released);
    assert!(replay.replayed);
}

/// Releasing one of two holds cannot permit mutation or release their shared pin.
#[test]
fn overlapping_holds_block_legacy_attempts_resume_and_pin_release_until_last_release() {
    let home = common::Home::new();
    let mut store = home.store();
    let target = target(&home, &mut store);
    store
        .conn
        .execute(
            "UPDATE agents SET runtime_session_id='held-session' WHERE id=?",
            [target.agent_id.as_str()],
        )
        .unwrap();
    let first_request = acquire(&store, "first", vec![target.clone()]);
    let second_request = acquire(&store, "second", vec![target.clone()]);
    let first = store.acquire_lineage_hold(&first_request).unwrap();
    let second = store.acquire_lineage_hold(&second_request).unwrap();
    for result in [&first, &second] {
        assert!(matches!(
            store.create_attempt(&target.agent_id, "prepared", &json!({})),
            Err(Error::Conflict)
        ));
        assert!(matches!(
            store.release_history_pin(&target.pin_id),
            Err(Error::Conflict)
        ));
        let parent = store.get(&target.agent_id).unwrap();
        assert!(matches!(
            store.admit(&home.request(), &home.config, &json!({}), Some(&parent)),
            Err(Error::Conflict)
        ));
        assert!(
            store
                .conn
                .execute(
                    "UPDATE history_pins SET released=1 WHERE pin_id=?",
                    [target.pin_id.as_str()]
                )
                .is_err()
        );
        assert!(
            store
                .conn
                .execute(
                    "UPDATE agents SET status='running' WHERE id=?",
                    [target.agent_id.as_str()]
                )
                .is_err()
        );
        assert!(
            store
                .conn
                .execute(
                    "UPDATE attempts SET state='running' WHERE agent_id=?",
                    [target.agent_id.as_str()]
                )
                .is_err()
        );
        assert!(store.conn.execute("INSERT INTO attempts(id,agent_id,number,state,adapter_state_json,created_at) VALUES('legacy-bypass',?,2,'prepared','{}',1)", [target.agent_id.as_str()]).is_err());
        // A fresh unrelated row cannot be moved into a held lineage through an UPDATE path.
        let unrelated = target_for_alias(&home, &mut store);
        assert!(
            store
                .conn
                .execute(
                    "UPDATE agents SET root_agent_id=?,parent_agent_id=?,sequence=2 WHERE id=?",
                    params![
                        target.agent_id.as_str(),
                        target.agent_id.as_str(),
                        unrelated.as_str()
                    ]
                )
                .is_err()
        );
        store
            .release_lineage_hold(
                &result.receipt.hold_id,
                &result.receipt.store_id,
                &signature(result),
            )
            .unwrap();
    }
    assert!(
        store
            .create_attempt(&target.agent_id, "prepared", &json!({}))
            .is_ok()
    );
    assert!(store.release_history_pin(&target.pin_id).unwrap().is_some());
}

/// Creates an unrelated row used to probe canonical-root alias mutation guards.
fn target_for_alias(home: &common::Home, store: &mut Store<'_>) -> AgentId {
    target(home, store).agent_id
}

/// Replacement conflicts must preserve held facts even when implicit DELETE triggers are disabled.
#[test]
fn replacement_conflicts_preserve_held_evidence_without_recursive_triggers() {
    let home = common::Home::new();
    let mut store = home.store();
    store
        .conn
        .pragma_update(None, "recursive_triggers", false)
        .unwrap();
    let root = target(&home, &mut store);
    let mut held = target(&home, &mut store);
    let other = target(&home, &mut store);
    store.conn.execute("INSERT INTO orchestrator_sessions(id,transport,external_session_id,created_at,last_seen_at) VALUES('hold-session','test','hold-session',1,1)", []).unwrap();
    store.conn.execute("UPDATE agents SET root_agent_id=?1,parent_agent_id=?1,sequence=2,orchestrator_session_id='hold-session',request_id='held-request' WHERE id=?2", params![root.agent_id.as_str(), held.agent_id.as_str()]).unwrap();
    held.sequence = 2;
    held.pin_id = root.pin_id.clone();
    store.conn.execute("INSERT INTO service_restarts(request_id,phase,started_at,boot_id) VALUES('held-restart','recovering',1,'fixture')", []).unwrap();
    let restart = store.conn.last_insert_rowid();
    store.conn.execute("INSERT INTO service_restarts(request_id,phase,started_at,boot_id,finished_at) VALUES('other-restart','complete',1,'fixture',2)", []).unwrap();
    let other_restart = store.conn.last_insert_rowid();
    store.conn.execute("INSERT INTO service_restart_agents(restart_id,agent_id,resume_status) VALUES(?,?,'finished')", params![restart, held.agent_id.as_str()]).unwrap();
    store.conn.execute("INSERT INTO service_restart_agents(restart_id,agent_id,resumed_execution_id,resume_status) VALUES(?,?,?,'finished')", params![restart, other.agent_id.as_str(), held.agent_id.as_str()]).unwrap();
    store.conn.execute("INSERT INTO service_restart_agents(restart_id,agent_id,resume_status) VALUES(?,?,'finished')", params![other_restart, other.agent_id.as_str()]).unwrap();
    let request = acquire(&store, "replacement-guards", vec![held.clone()]);
    let acquired = store.acquire_lineage_hold(&request).unwrap();
    assert!(
        store
            .conn
            .execute(
                "UPDATE lineage_holds SET rowid=rowid+1000,released=1 WHERE hold_id=?",
                [acquired.receipt.hold_id.as_str()]
            )
            .is_err()
    );
    let attempt: String = store
        .conn
        .query_row(
            "SELECT id FROM attempts WHERE agent_id=?",
            [held.agent_id.as_str()],
            |row| row.get(0),
        )
        .unwrap();
    let event: i64 = store
        .conn
        .query_row(
            "SELECT seq FROM events WHERE agent_id=? AND kind='status'",
            [held.agent_id.as_str()],
            |row| row.get(0),
        )
        .unwrap();
    let other_event = store
        .append_event(&other.agent_id, "note", &json!({}), None)
        .unwrap();
    let new_id = AgentId::new();
    let agent_insert = "INSERT OR REPLACE INTO agents(id,runtime,model,profile,task,task_summary,workdir,request_json,status,created_at,config_revision,root_agent_id,orchestrator_session_id,request_id,parent_agent_id) SELECT ?1,runtime,model,profile,task,task_summary,workdir,request_json,status,created_at,config_revision,?2,?3,?4,?5 FROM agents WHERE id=?2";
    let cases: Vec<(&str, Vec<rusqlite::types::Value>)> = vec![
        (
            "INSERT OR REPLACE INTO history_pins(pin_id,root_agent_id,released) VALUES(?,?,1)",
            vec![
                held.pin_id.to_string().into(),
                root.agent_id.to_string().into(),
            ],
        ),
        (
            "INSERT OR REPLACE INTO history_pins(rowid,pin_id,root_agent_id) SELECT rowid,?1,root_agent_id FROM history_pins WHERE pin_id=?2",
            vec![
                other.pin_id.to_string().into(),
                held.pin_id.to_string().into(),
            ],
        ),
        (
            "UPDATE OR REPLACE history_pins SET rowid=(SELECT rowid FROM history_pins WHERE pin_id=?1),released=1 WHERE pin_id=?2",
            vec![
                held.pin_id.to_string().into(),
                other.pin_id.to_string().into(),
            ],
        ),
        (
            "INSERT OR REPLACE INTO events(seq,agent_id,at,kind) VALUES(?,?,1,'note')",
            vec![event.into(), other.agent_id.to_string().into()],
        ),
        (
            "UPDATE OR REPLACE events SET seq=? WHERE seq=?",
            vec![event.into(), other_event.into()],
        ),
        (
            "INSERT OR REPLACE INTO attempts(id,agent_id,number,state,adapter_state_json,created_at) VALUES(?,?,2,'prepared','{}',1)",
            vec![attempt.clone().into(), other.agent_id.to_string().into()],
        ),
        (
            "UPDATE OR REPLACE attempts SET id=? WHERE agent_id=?",
            vec![attempt.into(), other.agent_id.to_string().into()],
        ),
        (
            "INSERT OR REPLACE INTO attempts(rowid,id,agent_id,number,state,adapter_state_json,created_at) SELECT rowid,'rowid-replacement',?1,2,'prepared','{}',1 FROM attempts WHERE agent_id=?2",
            vec![
                other.agent_id.to_string().into(),
                held.agent_id.to_string().into(),
            ],
        ),
        (
            "UPDATE OR REPLACE attempts SET _rowid_=(SELECT rowid FROM attempts WHERE agent_id=?1) WHERE agent_id=?2",
            vec![
                held.agent_id.to_string().into(),
                other.agent_id.to_string().into(),
            ],
        ),
        (
            "INSERT OR REPLACE INTO attempts(id,agent_id,number,state,adapter_state_json,created_at) VALUES('replacement-attempt',?,1,'prepared','{}',1)",
            vec![held.agent_id.to_string().into()],
        ),
        (
            agent_insert,
            vec![
                held.agent_id.to_string().into(),
                other.agent_id.to_string().into(),
                rusqlite::types::Value::Null,
                rusqlite::types::Value::Null,
                rusqlite::types::Value::Null,
            ],
        ),
        (
            agent_insert,
            vec![
                new_id.to_string().into(),
                other.agent_id.to_string().into(),
                "hold-session".to_owned().into(),
                "held-request".to_owned().into(),
                rusqlite::types::Value::Null,
            ],
        ),
        (
            agent_insert,
            vec![
                new_id.to_string().into(),
                other.agent_id.to_string().into(),
                rusqlite::types::Value::Null,
                rusqlite::types::Value::Null,
                root.agent_id.to_string().into(),
            ],
        ),
        (
            "UPDATE OR REPLACE agents SET id=? WHERE id=?",
            vec![
                held.agent_id.to_string().into(),
                other.agent_id.to_string().into(),
            ],
        ),
        (
            "INSERT OR REPLACE INTO agents(rowid,id,runtime,model,profile,task,task_summary,workdir,request_json,status,created_at,config_revision,root_agent_id) SELECT (SELECT rowid FROM agents WHERE id=?1),?2,runtime,model,profile,task,task_summary,workdir,request_json,status,created_at,config_revision,?2 FROM agents WHERE id=?3",
            vec![
                held.agent_id.to_string().into(),
                new_id.to_string().into(),
                other.agent_id.to_string().into(),
            ],
        ),
        (
            "UPDATE OR REPLACE agents SET oid=(SELECT rowid FROM agents WHERE id=?1) WHERE id=?2",
            vec![
                held.agent_id.to_string().into(),
                other.agent_id.to_string().into(),
            ],
        ),
        (
            "UPDATE OR REPLACE agents SET orchestrator_session_id='hold-session',request_id='held-request' WHERE id=?",
            vec![other.agent_id.to_string().into()],
        ),
        (
            "UPDATE OR REPLACE agents SET parent_agent_id=? WHERE id=?",
            vec![
                root.agent_id.to_string().into(),
                other.agent_id.to_string().into(),
            ],
        ),
        (
            "INSERT OR REPLACE INTO service_restart_agents(restart_id,agent_id,resume_status) VALUES(?,?,'finished')",
            vec![restart.into(), other.agent_id.to_string().into()],
        ),
        (
            "UPDATE OR REPLACE service_restart_agents SET restart_id=? WHERE restart_id=? AND agent_id=?",
            vec![
                restart.into(),
                other_restart.into(),
                other.agent_id.to_string().into(),
            ],
        ),
        (
            "INSERT OR REPLACE INTO service_restart_agents(rowid,restart_id,agent_id,resume_status) SELECT rowid,?1,?2,'finished' FROM service_restart_agents WHERE restart_id=?3 AND agent_id=?2",
            vec![
                other_restart.into(),
                other.agent_id.to_string().into(),
                restart.into(),
            ],
        ),
        (
            "UPDATE OR REPLACE service_restart_agents SET rowid=(SELECT rowid FROM service_restart_agents WHERE restart_id=?1 AND agent_id=?3) WHERE restart_id=?2 AND agent_id=?3",
            vec![
                restart.into(),
                other_restart.into(),
                other.agent_id.to_string().into(),
            ],
        ),
        (
            "UPDATE service_restart_agents SET resumed_execution_id=NULL WHERE restart_id=? AND agent_id=?",
            vec![restart.into(), other.agent_id.to_string().into()],
        ),
        (
            "DELETE FROM service_restart_agents WHERE restart_id=? AND agent_id=?",
            vec![restart.into(), other.agent_id.to_string().into()],
        ),
        (
            "INSERT OR REPLACE INTO service_restarts(id,request_id,phase,started_at,boot_id,finished_at) VALUES(?,'replacement-restart','complete',1,'fixture',2)",
            vec![restart.into()],
        ),
        (
            "INSERT OR REPLACE INTO service_restarts(request_id,phase,started_at,boot_id,finished_at) VALUES('held-restart','complete',1,'fixture',2)",
            vec![],
        ),
        (
            "INSERT OR REPLACE INTO service_restarts(request_id,phase,started_at,boot_id) VALUES('replacement-restart','recovering',1,'fixture')",
            vec![],
        ),
        (
            "UPDATE OR REPLACE service_restarts SET id=? WHERE id=?",
            vec![restart.into(), other_restart.into()],
        ),
        (
            "UPDATE OR REPLACE service_restarts SET request_id='held-restart' WHERE id=?",
            vec![other_restart.into()],
        ),
        (
            "UPDATE OR REPLACE service_restarts SET phase='recovering',finished_at=NULL WHERE id=?",
            vec![other_restart.into()],
        ),
    ];
    for (sql, values) in cases {
        let error = store
            .conn
            .execute(sql, rusqlite::params_from_iter(values))
            .unwrap_err();
        assert!(
            matches!(&error, rusqlite::Error::SqliteFailure(_, Some(message)) if message == "lineage has an active hold" || message == "history pin has an active lineage hold" || message == "history pin cannot be replaced" || message == "history pin rowid is immutable"),
            "{sql}: {error}"
        );
    }
    // Unrelated admissions and non-proof metadata/events on a held execution remain writable.
    store
        .conn
        .execute(
            "UPDATE service_restarts SET phase='complete',finished_at=2 WHERE id=?",
            [restart],
        )
        .unwrap();
    target(&home, &mut store);
    pin(&store, &root.agent_id);
    store
        .append_event(&held.agent_id, "note", &json!({}), None)
        .unwrap();
    store
        .conn
        .execute(
            "UPDATE agents SET request_id='renamed-request' WHERE id=?",
            [held.agent_id.as_str()],
        )
        .unwrap();
    store.conn.execute("INSERT INTO service_restarts(request_id,phase,started_at,boot_id,finished_at) VALUES('fresh-restart','complete',1,'fixture',2)", []).unwrap();
    assert_eq!(
        store.acquire_lineage_hold(&request).unwrap().receipt,
        acquired.receipt
    );
    let replace_identity = "INSERT OR REPLACE INTO store_identity(singleton,store_id) VALUES(1,'store_00000000000000000000000000000000')";
    assert!(
        store
            .conn
            .execute(replace_identity, [])
            .unwrap_err()
            .to_string()
            .contains("store identity cannot be replaced")
    );
    store
        .release_lineage_hold(
            &acquired.receipt.hold_id,
            &request.store_id,
            &signature(&acquired),
        )
        .unwrap();
    assert!(
        store
            .conn
            .execute(replace_identity, [])
            .unwrap_err()
            .to_string()
            .contains("store identity cannot be replaced")
    );
    assert_eq!(
        store.acquire_lineage_hold(&request).unwrap().state,
        LineageHoldState::Released
    );
    assert!(store.release_history_pin(&held.pin_id).unwrap().is_some());
    let released_pin_cases: Vec<(&str, Vec<rusqlite::types::Value>)> = vec![
        (
            "INSERT OR REPLACE INTO history_pins(pin_id,root_agent_id,released) SELECT pin_id,root_agent_id,0 FROM history_pins WHERE pin_id=?1",
            vec![held.pin_id.to_string().into()],
        ),
        (
            "INSERT OR REPLACE INTO history_pins(rowid,pin_id,root_agent_id) SELECT rowid,?2,root_agent_id FROM history_pins WHERE pin_id=?1",
            vec![
                held.pin_id.to_string().into(),
                other.pin_id.to_string().into(),
            ],
        ),
        (
            "UPDATE OR REPLACE history_pins SET rowid=(SELECT rowid FROM history_pins WHERE pin_id=?1),released=1 WHERE pin_id=?2",
            vec![
                held.pin_id.to_string().into(),
                other.pin_id.to_string().into(),
            ],
        ),
    ];
    for (sql, values) in released_pin_cases {
        let error = store
            .conn
            .execute(sql, rusqlite::params_from_iter(values))
            .unwrap_err();
        assert!(
            matches!(&error, rusqlite::Error::SqliteFailure(_, Some(message)) if message == "history pin cannot be replaced" || message == "history pin rowid is immutable"),
            "{sql}: {error}"
        );
    }
    assert!(
        store
            .release_history_pin(&held.pin_id)
            .unwrap()
            .unwrap()
            .already_released
    );
}

/// Released receipts replay after proof is deliberately changed; they never reacquire.
#[test]
fn released_replay_precedes_revalidation_and_tombstones_are_immutable() {
    let home = common::Home::new();
    let mut store = home.store();
    let target = target(&home, &mut store);
    let request = acquire(&store, "permanent", vec![target.clone()]);
    let acquired = store.acquire_lineage_hold(&request).unwrap();
    store
        .release_lineage_hold(
            &acquired.receipt.hold_id,
            &request.store_id,
            &signature(&acquired),
        )
        .unwrap();
    store.release_history_pin(&target.pin_id).unwrap();
    store
        .conn
        .execute(
            "UPDATE agents SET answer_sha256=NULL WHERE id=?",
            [target.agent_id.as_str()],
        )
        .unwrap();
    drop(store);
    let mut store = home.store();
    let replay = store.acquire_lineage_hold(&request).unwrap();
    assert!(replay.replayed);
    assert_eq!(replay.state, LineageHoldState::Released);
    assert_eq!(replay.receipt, acquired.receipt);
    for sql in [
        "DELETE FROM lineage_holds",
        "DELETE FROM lineage_hold_targets",
        "UPDATE lineage_holds SET released=0",
        "UPDATE lineage_holds SET fingerprint=zeroblob(32)",
        "UPDATE lineage_hold_targets SET sequence=2",
        "INSERT OR REPLACE INTO lineage_holds SELECT hold_id,request_id,version,fingerprint,verifying_key,intent_digest,target_count,0 FROM lineage_holds",
        "INSERT OR REPLACE INTO lineage_holds SELECT hold_id,'changed-request',version,fingerprint,verifying_key,intent_digest,target_count,0 FROM lineage_holds",
        "INSERT OR REPLACE INTO lineage_holds SELECT 'hold_22222222222242228222222222222222',request_id,version,fingerprint,verifying_key,intent_digest,target_count,0 FROM lineage_holds",
        "INSERT OR REPLACE INTO lineage_holds(rowid,hold_id,request_id,version,fingerprint,verifying_key,intent_digest,target_count,released) SELECT rowid,'hold_22222222222242228222222222222222','changed-request',version,fingerprint,verifying_key,intent_digest,target_count,0 FROM lineage_holds",
        "INSERT OR REPLACE INTO lineage_hold_targets SELECT * FROM lineage_hold_targets",
    ] {
        assert!(store.conn.execute(sql, []).is_err(), "{sql}");
    }
}

/// A new acquisition cannot replace another hold's permanent member through its hidden rowid.
#[test]
fn membership_rowid_conflicts_preserve_another_hold() {
    let home = common::Home::new();
    let mut store = home.store();
    store
        .conn
        .pragma_update(None, "recursive_triggers", false)
        .unwrap();
    let target = target(&home, &mut store);
    let request = acquire(&store, "original-membership", vec![target]);
    let acquired = store.acquire_lineage_hold(&request).unwrap();
    let new_hold = "hold_22222222222242228222222222222222";
    {
        let tx = store.conn.unchecked_transaction().unwrap();
        tx.execute("INSERT INTO lineage_holds(hold_id,request_id,version,fingerprint,verifying_key,target_count) SELECT ?1,'new-membership',version,fingerprint,verifying_key,target_count FROM lineage_holds WHERE hold_id=?2", params![new_hold, acquired.receipt.hold_id.as_str()]).unwrap();
        let error = tx.execute("INSERT OR REPLACE INTO lineage_hold_targets(rowid,hold_id,ordinal,agent_id,sequence,execution_ref,pin_id,root_agent_id) SELECT rowid,?1,ordinal,agent_id,sequence,execution_ref,pin_id,root_agent_id FROM lineage_hold_targets WHERE hold_id=?2", params![new_hold, acquired.receipt.hold_id.as_str()]).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("invalid lineage hold membership")
        );
        tx.execute("INSERT INTO lineage_hold_targets(hold_id,ordinal,agent_id,sequence,execution_ref,pin_id,root_agent_id) SELECT ?1,ordinal,agent_id,sequence,execution_ref,pin_id,root_agent_id FROM lineage_hold_targets WHERE hold_id=?2", params![new_hold, acquired.receipt.hold_id.as_str()]).unwrap();
        tx.rollback().unwrap();
    }
    assert_eq!(
        store.acquire_lineage_hold(&request).unwrap().receipt,
        acquired.receipt
    );
}

/// Wrong store and weak keys produce no hold or pin mutation.
#[test]
fn scope_and_invalid_key_are_fail_closed() {
    let home = common::Home::new();
    let mut store = home.store();
    let target = target(&home, &mut store);
    let mut request = acquire(&store, "bad", vec![target]);
    let original = request.store_id.clone();
    request.store_id = "store_00000000000000000000000000000001".parse().unwrap();
    assert!(matches!(
        store.acquire_lineage_hold(&request),
        Err(Error::StoreIdentityMismatch)
    ));
    request.store_id = original;
    request.verifying_key = [0; 32];
    assert!(store.acquire_lineage_hold(&request).is_err());
    assert_eq!(count(&store), 0);
}

/// Competing writers serialize the proof with pin release; no active hold can lose its pin.
#[test]
fn acquire_racing_pin_release_has_only_serial_outcomes() {
    let home = common::Home::new();
    let mut store = home.store();
    let target = target(&home, &mut store);
    let request = acquire(&store, "race", vec![target.clone()]);
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(3));
    let path = home.path.clone();
    let start = barrier.clone();
    let acquire_thread = std::thread::spawn(move || {
        let mut store = agent_run_store::fixture::open(&path).unwrap();
        start.wait();
        store.acquire_lineage_hold(&request)
    });
    let path = home.path.clone();
    let start = barrier.clone();
    let pin = target.pin_id.clone();
    let release_thread = std::thread::spawn(move || {
        let mut store = agent_run_store::fixture::open(&path).unwrap();
        start.wait();
        store.release_history_pin(&pin)
    });
    barrier.wait();
    let acquired = acquire_thread.join().unwrap();
    let released = release_thread.join().unwrap();
    assert_ne!(acquired.is_ok(), released.is_ok());
    let inconsistent: bool = store.conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM lineage_holds h JOIN lineage_hold_targets t USING(hold_id) \
         JOIN history_pins p USING(pin_id) WHERE h.released=0 AND p.released=1)", [], |row| row.get(0)).unwrap();
    assert!(!inconsistent);
}

/// Process compaction preserves pinned held evidence without performing a second lifecycle check.
#[test]
fn active_hold_keeps_process_compaction_ineligible() {
    let home = common::Home::new();
    let mut store = home.store();
    let target = target(&home, &mut store);
    let attempt: String = store
        .conn
        .query_row(
            "SELECT id FROM attempts WHERE agent_id=?",
            [target.agent_id.as_str()],
            |row| row.get(0),
        )
        .unwrap();
    store.conn.execute("INSERT INTO process_ownership(owner_kind,owner_id,leader_json,descendants_observed,updated_at) VALUES('attempt',?,'{}',1,1)", [&attempt]).unwrap();
    store.conn.execute("INSERT INTO process_members(owner_kind,owner_id,pid,token,identity_json) VALUES('attempt',?,234567,'fixture','{\"program\":\"fixture\"}')", [&attempt]).unwrap();
    let request = acquire(&store, "compact", vec![target.clone()]);
    let held = store.acquire_lineage_hold(&request).unwrap();
    let tx = agent_run_store::writer::WriteTx::begin(&store.conn).unwrap();
    assert!(
        !agent_run_store::process_ownership::compact_terminal_processes(
            &tx,
            target.agent_id.as_str()
        )
        .unwrap()
    );
    tx.commit().unwrap();
    assert_eq!(
        store
            .conn
            .query_row(
                "SELECT count(*) FROM process_members WHERE owner_id=?",
                [&attempt],
                |row| row.get::<_, i64>(0)
            )
            .unwrap(),
        1
    );
    store
        .release_lineage_hold(&held.receipt.hold_id, &request.store_id, &signature(&held))
        .unwrap();
    store.release_history_pin(&target.pin_id).unwrap();
    let tx = agent_run_store::writer::WriteTx::begin(&store.conn).unwrap();
    assert!(
        agent_run_store::process_ownership::compact_terminal_processes(
            &tx,
            target.agent_id.as_str()
        )
        .unwrap()
    );
    tx.commit().unwrap();
    assert_eq!(
        store
            .conn
            .query_row(
                "SELECT count(*) FROM process_members WHERE owner_id=?",
                [&attempt],
                |row| row.get::<_, i64>(0)
            )
            .unwrap(),
        0
    );
}

/// An injected failure after the first membership insert leaves neither header nor targets.
#[test]
fn interrupted_membership_insert_rolls_back_entire_hold() {
    let home = common::Home::new();
    let mut store = home.store();
    let targets = (0..2).map(|_| target(&home, &mut store)).collect();
    let request = acquire(&store, "rollback", targets);
    store.conn.execute_batch("CREATE TRIGGER fail_second_hold_member BEFORE INSERT ON lineage_hold_targets WHEN NEW.ordinal=1 BEGIN SELECT RAISE(ABORT,'injected membership failure'); END;").unwrap();
    assert!(store.acquire_lineage_hold(&request).is_err());
    assert_eq!(count(&store), 0);
    assert_eq!(
        store
            .conn
            .query_row("SELECT count(*) FROM lineage_hold_targets", [], |row| row
                .get::<_, i64>(
                0
            ))
            .unwrap(),
        0
    );
    store
        .conn
        .execute_batch("DROP TRIGGER fail_second_hold_member")
        .unwrap();
    assert!(!store.acquire_lineage_hold(&request).unwrap().replayed);
}

/// The supported upper bound preserves all twenty targets in caller order.
#[test]
fn twenty_targets_are_atomic_and_twenty_one_are_rejected() {
    let home = common::Home::new();
    let mut store = home.store();
    let mut targets: Vec<_> = (0..20).map(|_| target(&home, &mut store)).collect();
    targets.reverse();
    let request = acquire(&store, "twenty", targets);
    let held = store.acquire_lineage_hold(&request).unwrap();
    assert_eq!(held.receipt.targets, request.targets);
    let mut oversized = request;
    oversized.request_id = "twenty-one".into();
    oversized.targets.push(target(&home, &mut store));
    assert!(store.acquire_lineage_hold(&oversized).is_err());
    assert_eq!(count(&store), 1);
}

/// A competing legacy attempt either precedes the proof and invalidates it, or is blocked by the hold.
#[test]
fn acquire_racing_attempt_creation_has_only_serial_outcomes() {
    let home = common::Home::new();
    let mut store = home.store();
    let target = target(&home, &mut store);
    let request = acquire(&store, "attempt-race", vec![target.clone()]);
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(3));
    let path = home.path.clone();
    let start = barrier.clone();
    let acquire_thread = std::thread::spawn(move || {
        let mut store = agent_run_store::fixture::open(&path).unwrap();
        start.wait();
        store.acquire_lineage_hold(&request)
    });
    let path = home.path.clone();
    let start = barrier.clone();
    let agent = target.agent_id.clone();
    let attempt_thread = std::thread::spawn(move || {
        let mut store = agent_run_store::fixture::open(&path).unwrap();
        start.wait();
        store.create_attempt(&agent, "prepared", &json!({}))
    });
    barrier.wait();
    let acquired = acquire_thread.join().unwrap();
    let attempted = attempt_thread.join().unwrap();
    assert_ne!(acquired.is_ok(), attempted.is_ok());
    let attempts: i64 = store
        .conn
        .query_row(
            "SELECT count(*) FROM attempts WHERE agent_id=?",
            [target.agent_id.as_str()],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(attempts, if acquired.is_ok() { 1 } else { 2 });
}

/// Recovery lookup is read-only, exact-scope/fingerprint fenced, and preserves released receipts.
#[test]
fn recovery_lookup_is_read_only_and_exact_for_active_and_released_holds() {
    let home = common::Home::new();
    let mut store = home.store();
    let target = target(&home, &mut store);
    let request = acquire(&store, "recovery", vec![target]);
    let held = store.acquire_lineage_hold(&request).unwrap();
    let reader = home.store();
    reader.conn.pragma_update(None, "query_only", true).unwrap();
    let observed = reader
        .lookup_lineage_hold(
            &request.store_id,
            &request.request_id,
            &held.receipt.fingerprint,
        )
        .unwrap()
        .unwrap();
    assert_eq!(observed.receipt, held.receipt);
    assert_eq!(observed.state, LineageHoldState::Active);
    assert!(
        reader
            .lookup_lineage_hold(&request.store_id, "never-acquired", &[0; 32])
            .unwrap()
            .is_none()
    );
    assert!(matches!(
        reader.lookup_lineage_hold(&request.store_id, &request.request_id, &[0; 32]),
        Err(Error::Conflict)
    ));
    let foreign = "store_00000000000000000000000000000001".parse().unwrap();
    assert!(matches!(
        reader.lookup_lineage_hold(&foreign, "never-acquired", &[0; 32]),
        Err(Error::StoreIdentityMismatch)
    ));
    store
        .release_lineage_hold(&held.receipt.hold_id, &request.store_id, &signature(&held))
        .unwrap();
    let released = reader
        .lookup_lineage_hold(
            &request.store_id,
            &request.request_id,
            &held.receipt.fingerprint,
        )
        .unwrap()
        .unwrap();
    assert_eq!(released.receipt, held.receipt);
    assert_eq!(released.state, LineageHoldState::Released);
    assert!(matches!(
        reader.lookup_lineage_hold(&request.store_id, &request.request_id, &[0; 32]),
        Err(Error::Conflict)
    ));
    assert_eq!(count(&store), 1);
}

/// An absent lookup does not exclude an in-flight acquisition that later commits.
#[test]
fn recovery_lookup_absence_is_snapshot_history_not_inflight_proof() {
    let home = common::Home::new();
    let mut store = home.store();
    let target = target(&home, &mut store);
    let request = acquire(&store, "inflight", vec![target]);
    let fingerprint = request.fingerprint().unwrap();
    let reader = home.store();
    reader.conn.pragma_update(None, "query_only", true).unwrap();
    store.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
    let held = store.acquire_lineage_hold(&request).unwrap();
    assert!(!store.conn.is_autocommit());
    assert!(
        reader
            .lookup_lineage_hold(&request.store_id, &request.request_id, &fingerprint)
            .unwrap()
            .is_none()
    );
    assert!(
        reader
            .list_lineage_holds(&request.store_id, LineageHoldFilter::All, 100, None)
            .unwrap()
            .holds
            .is_empty()
    );
    store.conn.execute_batch("COMMIT").unwrap();
    assert_eq!(
        reader
            .lookup_lineage_hold(&request.store_id, &request.request_id, &fingerprint)
            .unwrap()
            .unwrap()
            .receipt,
        held.receipt
    );
}

/// Inventory uses exclusive ID keysets, complete receipts and correct active/released filtering.
#[test]
fn recovery_inventory_pages_without_duplicates_or_truncating_targets() {
    let home = common::Home::new();
    let mut store = home.store();
    let targets = vec![target(&home, &mut store), target(&home, &mut store)];
    let mut acquired: Vec<_> = (0..3)
        .map(|index| {
            let request = acquire(&store, &format!("inventory-{index}"), targets.clone());
            store.acquire_lineage_hold(&request).unwrap()
        })
        .collect();
    acquired.sort_by(|left, right| {
        left.receipt
            .hold_id
            .as_str()
            .cmp(right.receipt.hold_id.as_str())
    });
    let released = &acquired[1];
    let store_id = released.receipt.store_id.clone();
    store
        .release_lineage_hold(&released.receipt.hold_id, &store_id, &signature(released))
        .unwrap();
    store.conn.pragma_update(None, "query_only", true).unwrap();
    let mut after = None;
    for (index, expected) in acquired.iter().enumerate() {
        let page = store
            .list_lineage_holds(&store_id, LineageHoldFilter::All, 1, after.as_ref())
            .unwrap();
        assert_eq!(page.holds.len(), 1);
        assert_eq!(page.holds[0].receipt, expected.receipt);
        assert_eq!(page.holds[0].receipt.targets.len(), 2);
        assert_eq!(
            page.holds[0].state,
            if index == 1 {
                LineageHoldState::Released
            } else {
                LineageHoldState::Active
            }
        );
        assert_eq!(
            page.next_after_hold_id,
            if index < 2 {
                Some(expected.receipt.hold_id.clone())
            } else {
                None
            }
        );
        after = page.next_after_hold_id;
    }
    let active = store
        .list_lineage_holds(&store_id, LineageHoldFilter::Active, 100, None)
        .unwrap();
    assert_eq!(
        active
            .holds
            .iter()
            .map(|hold| hold.receipt.hold_id.clone())
            .collect::<Vec<_>>(),
        vec![
            acquired[0].receipt.hold_id.clone(),
            acquired[2].receipt.hold_id.clone()
        ]
    );
    assert!(active.next_after_hold_id.is_none());
    let page = store
        .list_lineage_holds(&store_id, LineageHoldFilter::Released, 1, None)
        .unwrap();
    assert_eq!(page.holds.len(), 1);
    assert_eq!(page.holds[0].receipt, released.receipt);
    assert!(page.next_after_hold_id.is_none());
    let empty = store
        .list_lineage_holds(
            &store_id,
            LineageHoldFilter::All,
            1,
            Some(&acquired[2].receipt.hold_id),
        )
        .unwrap();
    assert!(empty.holds.is_empty());
    assert!(empty.next_after_hold_id.is_none());
}

/// Invalid pagination/scope/request inputs never turn into an unbounded or mutating query.
#[test]
fn recovery_inventory_and_lookup_validate_bounds_and_store_scope() {
    let home = common::Home::new();
    let store = home.store();
    let store_id = agent_run_store::store_identity::read(&store.conn).unwrap();
    store.conn.pragma_update(None, "query_only", true).unwrap();
    for limit in [0, 101, usize::MAX] {
        assert!(
            store
                .list_lineage_holds(&store_id, LineageHoldFilter::All, limit, None)
                .is_err()
        );
    }
    let foreign = "store_00000000000000000000000000000001".parse().unwrap();
    assert!(matches!(
        store.list_lineage_holds(&foreign, LineageHoldFilter::All, 1, None),
        Err(Error::StoreIdentityMismatch)
    ));
    for request in [
        String::new(),
        " ".into(),
        "a\0b".into(),
        "a".repeat(513),
        "😀".repeat(513),
    ] {
        assert!(
            store
                .lookup_lineage_hold(&store_id, &request, &[0; 32])
                .is_err()
        );
    }
    let page = store
        .list_lineage_holds(&store_id, LineageHoldFilter::All, 100, None)
        .unwrap();
    assert!(page.holds.is_empty());
    assert!(page.next_after_hold_id.is_none());
    assert_eq!(count(&store), 0);
}
