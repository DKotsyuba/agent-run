//! Durable ownership and conflict checks using inert identities rather than spawned processes.

mod common;

use agent_run_platform::process::{Identity, OwnedProcess, OwnershipSnapshot};
use agent_run_store::Store;

/// Constructs inert process evidence; tests never signal these identities.
fn identity(pid: i32, token: &str) -> Identity {
    Identity {
        pid,
        ppid: 2,
        group: 10,
        birth: 123.0,
        token: token.into(),
        program: None,
        zombie: false,
    }
}

/// Creates a durable service owner without starting any command.
fn generation(store: &Store, id: &str) {
    store.conn.execute("INSERT INTO managed_service_generations(id,service_id,revision,definition_json,state,broker_identity_json,created_at) VALUES (?1,?1,?2,'{}','starting','{}',1)", rusqlite::params![id,"0".repeat(64)]).unwrap();
}

/// Captured members survive reopening, retain PID reuse generations and cannot acquire a second owner.
#[test]
fn restored_members_are_durable_and_owner_conflicts_roll_back() {
    let home = tempfile::tempdir().unwrap();
    agent_run_store::fixture::initialize(home.path()).unwrap();
    let mut store = agent_run_store::fixture::open(home.path()).unwrap();
    generation(&store, "first");
    generation(&store, "second");
    let root = identity(10, "root");
    let old_child = identity(11, "child-old");
    let new_child = identity(11, "child-new");
    let snapshot = OwnershipSnapshot {
        leader: root.clone(),
        members: vec![root, old_child, new_child],
        descendants_observed: true,
    };
    store
        .remember_processes("service", "first", &snapshot)
        .unwrap();
    store
        .remember_processes("service", "first", &snapshot)
        .unwrap();
    assert!(
        store
            .remember_processes("service", "second", &snapshot)
            .is_err()
    );
    assert!(
        store
            .remembered_processes("service", "second")
            .unwrap()
            .is_none()
    );
    drop(store);
    let store = agent_run_store::fixture::open(home.path()).unwrap();
    let snapshot = store
        .remembered_processes("service", "first")
        .unwrap()
        .unwrap()
        .snapshot()
        .unwrap();
    assert_eq!(snapshot.members.len(), 3);
    assert!(snapshot.descendants_observed);
    assert!(
        store
            .remembered_processes("attempt", "historical-with-no-snapshot")
            .unwrap()
            .is_none()
    );
    let mut broken = snapshot.clone();
    broken.members.clear();
    assert!(OwnedProcess::restore(broken).is_err());
    let mut broken = snapshot;
    broken.members.push(identity(1, "init"));
    assert!(OwnedProcess::restore(broken).is_err());
}

/// A history well above the former two-MiB ceiling survives bounded, replay-safe deltas and reopen.
#[test]
fn large_history_uses_bounded_deltas_without_a_total_limit() {
    let home = tempfile::tempdir().unwrap();
    agent_run_store::fixture::initialize(home.path()).unwrap();
    let mut store = agent_run_store::fixture::open(home.path()).unwrap();
    generation(&store, "large");
    let leader = identity(10, "root");
    let mut members = vec![leader.clone()];
    members
        .extend((100..20_100).map(|pid| identity(pid, &format!("child-{pid}-{}", "x".repeat(96)))));
    let snapshot = OwnershipSnapshot {
        leader,
        members,
        descendants_observed: true,
    };
    assert!(serde_json::to_vec(&snapshot).unwrap().len() > 2 * 1024 * 1024);
    let owner = OwnedProcess::restore(snapshot.clone()).unwrap();
    let mut offset = 0;
    while offset < owner.capture_revision().0 {
        let (delta, end) = owner.snapshot_since(offset).unwrap();
        assert!(end > offset && delta.members.len() <= 513);
        assert!(serde_json::to_vec(&delta).unwrap().len() < 512 * 1024);
        store
            .remember_processes("service", "large", &delta)
            .unwrap();
        store
            .remember_processes("service", "large", &delta)
            .unwrap();
        offset = end;
    }
    // Local recovery may merge a full historical snapshot; only individual wire batches are bounded.
    store
        .remember_processes("service", "large", &snapshot)
        .unwrap();
    drop(store);
    let store = agent_run_store::fixture::open(home.path()).unwrap();
    assert_eq!(
        store
            .remembered_processes("service", "large")
            .unwrap()
            .unwrap()
            .capture_revision()
            .0,
        20_001
    );
    assert_eq!(owner.snapshot_since(offset).unwrap().0.members.len(), 1);
}

/// Terminal status alone never deletes ownership; verified cleanup atomically retains a small summary.
#[test]
fn terminal_compaction_requires_cleanup_and_is_atomic_and_idempotent() {
    use agent_run_domain::domain::Outcome;
    use agent_run_store::process_ownership::compact_terminal_processes;
    use agent_run_store::writer::WriteTx;
    use serde_json::json;
    let home = common::Home::new();
    let mut store = home.store();
    assert!(
        agent_run_core::housekeeping::compact_process_artifacts(
            &home.path,
            &store.conn,
            "../outside",
            64
        )
        .is_err()
    );
    let (id, _) = store
        .admit(&home.request(), &home.config, &json!({}), None)
        .unwrap();
    let attempt = store.create_attempt(&id, "running", &json!({})).unwrap();
    store
        .conn
        .execute(
            "UPDATE attempts SET ownership_active=1 WHERE id=?",
            [&attempt],
        )
        .unwrap();
    let mut leader = identity(10, "root");
    leader.program = Some("cargo".into());
    let mut members = vec![leader.clone(), identity(11, "old-unknown")];
    members.extend((12..40).map(|pid| {
        let mut p = identity(pid, &format!("child-{pid}"));
        p.program = Some(if pid < 15 {
            "rustc".into()
        } else {
            format!("tool-{pid}")
        });
        p
    }));
    members.push({
        let mut p = identity(40, "rustc");
        p.program = Some("rustc".into());
        p
    });
    let snapshot = OwnershipSnapshot {
        leader,
        members,
        descendants_observed: true,
    };
    store
        .remember_processes("attempt", &attempt, &snapshot)
        .unwrap();
    {
        let tx = WriteTx::begin(&store.conn).unwrap();
        assert!(!compact_terminal_processes(&tx, id.as_str()).unwrap());
        tx.commit().unwrap();
    }
    store
        .finish(&id, &Outcome::failure("fixture"), None, None)
        .unwrap();
    assert!(
        store
            .remembered_processes("attempt", &attempt)
            .unwrap()
            .is_some()
    );
    store.conn.execute("UPDATE attempts SET ownership_active=0,phase='cleanup_complete',cleanup_proof_json='{\"confirmed\":true}' WHERE id=?",[&attempt]).unwrap();
    // A rolled-back summary must not lose details or become a duplicate on retry.
    {
        let tx = WriteTx::begin(&store.conn).unwrap();
        assert!(compact_terminal_processes(&tx, id.as_str()).unwrap());
    }
    assert!(
        store
            .remembered_processes("attempt", &attempt)
            .unwrap()
            .is_some()
    );
    {
        let tx = WriteTx::begin(&store.conn).unwrap();
        assert!(compact_terminal_processes(&tx, id.as_str()).unwrap());
        assert!(!compact_terminal_processes(&tx, id.as_str()).unwrap());
        tx.commit().unwrap();
    }
    assert!(
        store
            .remembered_processes("attempt", &attempt)
            .unwrap()
            .is_none()
    );
    assert!(
        store
            .remember_processes("attempt", &attempt, &snapshot)
            .is_err(),
        "a delayed batch cannot resurrect a closed history"
    );
    let body: String = store
        .conn
        .query_row(
            "SELECT data_json FROM events WHERE agent_id=? AND kind='process_summary_v1'",
            [id.as_str()],
            |r| r.get(0),
        )
        .unwrap();
    assert!(body.len() < 4096);
    let summary: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(summary["observed_processes"], 31);
    assert_eq!(summary["unknown_program"], 1);
    assert_eq!(summary["programs"].as_array().unwrap().len(), 16);
    assert_eq!(summary["other_programs"], 11);
    assert!(
        summary["programs"]
            .as_array()
            .unwrap()
            .iter()
            .any(|p| p["name"] == "rustc" && p["count"] == 4)
    );
    assert_eq!(summary["cleanup_confirmed"], true);
    let directory = home.path.join("agents").join(id.as_str());
    agent_run_platform::fs::private_dir(&directory).unwrap();
    let recovery = directory.join(format!("recovery.{attempt}.json"));
    std::fs::write(&recovery, b"{}").unwrap();
    let candidate = directory.join(format!("recovery.{attempt}.answer.md"));
    std::fs::write(&candidate, b"candidate answer").unwrap();
    agent_run_core::housekeeping::compact_process_artifacts(
        &home.path,
        &store.conn,
        id.as_str(),
        64,
    )
    .unwrap();
    assert!(!recovery.exists());
    assert!(candidate.exists());
    // A failed execution's unaccepted candidate must keep its seal after process details are discarded.
    std::fs::write(&recovery,json!({"candidate":{"path":"candidate.md","sha256":"0".repeat(64),"bytes":16},"ownership":{"members":[1,2,3]}}).to_string()).unwrap();
    agent_run_core::housekeeping::compact_process_artifacts(
        &home.path,
        &store.conn,
        id.as_str(),
        64,
    )
    .unwrap();
    let compact: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&recovery).unwrap()).unwrap();
    assert!(compact.get("ownership").is_none());
    assert!(compact.get("candidate").is_some());
}

/// Old JSON stays readable and basename validation rejects paths/control data without altering identity fences.
#[test]
fn program_metadata_is_optional_and_names_are_basenames() {
    let mut root = identity(10, "root");
    let old = serde_json::to_value(&root).unwrap();
    assert!(old.get("program").is_none());
    assert!(
        serde_json::from_value::<Identity>(old)
            .unwrap()
            .program
            .is_none()
    );
    root.program = Some("/private/token".into());
    assert!(
        OwnedProcess::restore(OwnershipSnapshot {
            leader: root.clone(),
            members: vec![root],
            descendants_observed: true
        })
        .is_err()
    );
    let owner = OwnedProcess::capture(std::process::id() as i32);
    assert!(owner.snapshot().unwrap().leader.program.is_some());
}

/// Normal success, failure and cancellation all compact only after verified cleanup in the terminal commit.
#[test]
fn terminal_commit_compacts_each_proven_outcome() {
    use agent_run_domain::domain::{Outcome, Status};
    use serde_json::json;
    for status in [Status::Succeeded, Status::Failed, Status::Cancelled] {
        let home = common::Home::new();
        let mut store = home.store();
        let mut request = home.request();
        request.explicit_finish = false;
        let (id, _) = store
            .admit(&request, &home.config, &json!({}), None)
            .unwrap();
        store.running(&id, 10).unwrap();
        let attempt = format!("{id}:1");
        let leader = identity(10, "root");
        store
            .remember_processes(
                "attempt",
                &attempt,
                &OwnershipSnapshot {
                    leader: leader.clone(),
                    members: vec![leader],
                    descendants_observed: true,
                },
            )
            .unwrap();
        store.conn.execute("UPDATE attempts SET phase='cleanup_complete',ownership_active=1,cleanup_proof_json=? WHERE id=?",
            rusqlite::params![json!({"confirmed":true,"group_gone":true,"descendants_gone":true}).to_string(),attempt]).unwrap();
        let directory = home.path.join("agents").join(id.as_str());
        agent_run_platform::fs::private_dir(&directory).unwrap();
        let proof = agent_run_platform::verify::seal(
            &directory,
            std::path::Path::new("answer.md"),
            "answer",
        )
        .unwrap();
        let mut outcome = Outcome::failure("fixture");
        outcome.status = status;
        store
            .finish(
                &id,
                &outcome,
                if status == Status::Succeeded {
                    Some(&proof)
                } else {
                    None
                },
                None,
            )
            .unwrap();
        assert!(
            store
                .remembered_processes("attempt", &attempt)
                .unwrap()
                .is_none()
        );
        assert_eq!(
            store
                .conn
                .query_row(
                    "SELECT COUNT(*) FROM events WHERE agent_id=? AND kind='process_summary_v1'",
                    [id.as_str()],
                    |r| r.get::<_, i64>(0)
                )
                .unwrap(),
            1
        );
    }
}
