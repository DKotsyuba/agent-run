//! Durable recovery of an observed native command result: the recovery
//! material exists before any failable storage wait after the effect, is kept
//! in a private owned file, and is settled as storage evidence only, never as
//! a repeated native action.
mod common;
use agent_run_core::persist::Persist;
use agent_run_domain::domain::AgentId;
use agent_run_store::ops::JournalFragment;
use serde_json::{Value, json};
use std::os::unix::fs::MetadataExt;

/// A running legacy execution (attempt `<id>:1`, no bound attempt) with one
/// claimed steering command, served by a real writer.
async fn claimed() -> (common::Home, common::writer::Harness, Persist, AgentId, i64) {
    let home = common::Home::new();
    let (id, _) = home
        .store()
        .admit(&home.request(), &home.config, &json!({}), None)
        .unwrap();
    let mut store = home.store();
    let attempt = format!("{id}:1");
    store.conn.execute("INSERT INTO attempts(id,agent_id,number,state,adapter_state_json,created_at,ownership_active) VALUES(?,?,1,'running','{}',100,1)", rusqlite::params![attempt, id.as_str()]).unwrap();
    store
        .conn
        .execute(
            "UPDATE agents SET status='running' WHERE id=?",
            [id.as_str()],
        )
        .unwrap();
    store.enqueue(&id, "steer", &json!({"text": "go"})).unwrap();
    let prep = common::writer::prepare_supervisor(&mut store, &id);
    drop(store);
    let harness = common::writer::Harness::start(&home.path);
    let mut persist = harness.supervisor(&home.path, prep, &id).await;
    let (command, kind, _) = persist
        .claim_command(&id)
        .await
        .unwrap()
        .expect("the steer command is claimable");
    assert_eq!(kind, "steer");
    (home, harness, persist, id, command)
}

/// Lines of the execution's private recovery log.
fn log_lines(home: &common::Home, id: &AgentId) -> Vec<Value> {
    let path = home
        .path
        .join("agents")
        .join(id.as_str())
        .join("recovery-observations.jsonl");
    std::fs::read_to_string(path)
        .unwrap_or_default()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}

/// Confirm row41 stores one unverified observation linked to the rejected original, without replaying any native/business effects.
fn assert_unverified_only(
    home: &common::Home,
    persist: &Persist,
    id: &AgentId,
    command: i64,
    bad_kind: &str,
) {
    let lines = log_lines(home, id);
    let identities: Vec<_> = lines
        .iter()
        .filter(|line| line["line"] == "identity")
        .collect();
    assert_eq!(
        identities.len(),
        1,
        "original operation identity must not be rebuilt"
    );
    let identity = identities[0];
    let original = identity["operation_id"].as_str().unwrap();
    let digest = identity["digest"].as_str().unwrap();
    let receipt: (String, String) = persist
        .conn
        .query_row(
            "SELECT digest,outcome FROM operation_receipts WHERE operation_id=?",
            [original],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(receipt, (digest.to_owned(), "rejected".to_owned()));
    assert_eq!(
        persist
            .conn
            .query_row(
                "SELECT count(*) FROM operation_receipts WHERE op_kind=?",
                [agent_run_store::ops::OpKindRef::ObservedCommandResult.name()],
                |row| row.get::<_, i64>(0)
            )
            .unwrap(),
        1
    );
    let events: Vec<String> = persist
        .conn
        .prepare("SELECT data_json FROM events WHERE agent_id=? AND kind='recovery_observation_v1'")
        .unwrap()
        .query_map([id.as_str()], |row| row.get(0))
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap();
    assert_eq!(
        events.len(),
        1,
        "one original has one durable unverified observation"
    );
    let event: Value = serde_json::from_str(&events[0]).unwrap();
    assert_eq!(event["status"], "recovered_unverified");
    assert_eq!(event["original_operation_id"], original);
    assert_eq!(event["original_digest"], digest);
    assert_eq!(event["original_committed"], false);
    assert_eq!(
        persist
            .conn
            .query_row("SELECT state FROM commands WHERE id=?", [command], |row| {
                row.get::<_, String>(0)
            })
            .unwrap(),
        "claimed"
    );
    assert_eq!(
        persist
            .conn
            .query_row(
                "SELECT count(*) FROM events WHERE agent_id=? AND kind=?",
                rusqlite::params![id.as_str(), bad_kind],
                |row| row.get::<_, i64>(0)
            )
            .unwrap(),
        0
    );
    assert_eq!(persist.conn.query_row("SELECT COALESCE(sum(turn_count),0) FROM worker_lifecycle WHERE attempt_id IN (SELECT id FROM attempts WHERE agent_id=?)", [id.as_str()], |row| row.get::<_, i64>(0)).unwrap(), 0);
    assert_eq!(
        persist
            .conn
            .query_row(
                "SELECT status FROM agents WHERE id=?",
                [id.as_str()],
                |row| row.get::<_, String>(0)
            )
            .unwrap(),
        "running",
        "unverified observation must not finish the execution"
    );
}

/// A normal observed result leaves a pending line, its identity and its
/// resolution in a private mode-0600 regular file, and nothing unresolved.
#[tokio::test]
async fn a_committed_observation_resolves_its_private_recovery_entry() {
    let (home, _harness, mut persist, id, command) = claimed().await;
    persist
        .observed_command_result(
            &id,
            command,
            &json!({"delivered": true}),
            "stdin_written",
            None,
            vec![],
        )
        .await
        .expect("evidence committed");
    assert_eq!(persist.unresolved_recoveries(), 0);
    let lines = log_lines(&home, &id);
    let kinds: Vec<&str> = lines.iter().map(|l| l["line"].as_str().unwrap()).collect();
    assert_eq!(kinds, ["pending", "identity", "resolved"], "{lines:?}");
    // The legacy attempt association is recorded although no attempt is bound.
    assert_eq!(lines[0]["attempt"], json!(format!("{id}:1")));
    let metadata = std::fs::metadata(
        home.path
            .join("agents")
            .join(id.as_str())
            .join("recovery-observations.jsonl"),
    )
    .unwrap();
    assert!(metadata.is_file());
    assert_eq!(metadata.mode() & 0o777, 0o600);
    assert_eq!(metadata.nlink(), 1);
}

/// A refused evidence write (here: a journal row of the same operation that the
/// writer durably refuses) keeps the recovery entry with the original identity
/// and outcome in the private log. Recording it as unverified evidence needs the
/// writer's recovery-observation row. Once that row is durable the pending
/// observation resolves as unverified; neither the native effect nor the refused
/// operation is applied again, and no successful outcome is inferred.
#[tokio::test]
async fn a_refused_observation_is_kept_and_never_repeated() {
    let (home, _harness, mut persist, id, command) = claimed().await;
    // A fragment naming an attempt that does not exist is refused as a whole.
    let bad = JournalFragment::Event {
        kind: "bad_row".into(),
        data: json!({}),
        attempt: Some("no-such-attempt".into()),
    };
    persist
        .observed_command_result(
            &id,
            command,
            &json!({"delivered": true}),
            "stdin_written",
            None,
            vec![bad],
        )
        .await
        .expect_err("the writer refuses the operation");
    assert_eq!(persist.unresolved_recoveries(), 1, "the entry is kept");
    let lines = log_lines(&home, &id);
    assert_eq!(lines[0]["line"], "pending");
    assert!(lines.iter().any(|l| l["line"] == "identity"), "{lines:?}");
    assert!(lines.iter().any(|l| l["line"] == "outcome"), "{lines:?}");
    assert_eq!(persist.drain_recoveries().await.unwrap(), 1);
    assert_eq!(persist.unresolved_recoveries(), 0);
    assert_eq!(persist.drain_recoveries().await.unwrap(), 0);
    assert_eq!(
        log_lines(&home, &id)
            .iter()
            .filter(|line| line["line"] == "resolved")
            .count(),
        1
    );
    assert_unverified_only(&home, &persist, &id, command, "bad_row");
    let bad_rows: i64 = persist
        .conn
        .query_row(
            "SELECT COUNT(*) FROM events WHERE agent_id=? AND kind='bad_row'",
            [id.as_str()],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(bad_rows, 0, "the refused rows never entered the journal");
}

/// A failure BEFORE the evidence operation is built or sent (the pre-flush of
/// buffered rows is refused) still leaves the recovery material durable, and a
/// later drain submits the evidence once.
#[tokio::test]
async fn a_pre_flush_failure_keeps_durable_recovery_material() {
    let (home, _harness, mut persist, id, command) = claimed().await;
    // A buffered row that the writer refuses makes the pre-flush fail.
    persist
        .event(&id, "buffered", &json!({}))
        .await
        .expect("buffered");
    persist.bind_attempt("no-such-attempt");
    persist
        .event(&id, "buffered_bad", &json!({}))
        .await
        .expect("buffered");
    persist.bind_attempt(&format!("{id}:1"));
    let error = persist
        .observed_command_result(
            &id,
            command,
            &json!({"delivered": true}),
            "stdin_written",
            None,
            vec![],
        )
        .await;
    assert!(error.is_err(), "the pre-flush fails");
    let lines = log_lines(&home, &id);
    assert_eq!(lines[0]["line"], "pending", "durable before the failure");
    assert!(
        !lines.iter().any(|l| l["line"] == "identity"),
        "nothing was built or sent: {lines:?}"
    );
    assert_eq!(persist.unresolved_recoveries(), 1);
    assert_eq!(
        persist.drain_recoveries().await.unwrap(),
        1,
        "submitted exactly once"
    );
    assert_eq!(persist.unresolved_recoveries(), 0);
}

/// Unsafe recovery targets fail before submission and retain the observed
/// result in memory. Removing only that target permits one evidence submission;
/// a repeated drain is empty and never writes the claimed command twice.
#[tokio::test]
async fn unsafe_recovery_files_refuse_before_send_and_remain_recoverable() {
    use std::os::unix::fs::{PermissionsExt, symlink};
    for kind in ["public", "symlink", "hardlink", "directory"] {
        let (home, _harness, mut persist, id, command) = claimed().await;
        let directory = home.path.join("agents").join(id.as_str());
        agent_run_platform::fs::private_dir(&directory).unwrap();
        let path = directory.join("recovery-observations.jsonl");
        let target = home.path.join("untouched");
        std::fs::write(&target, b"unchanged").unwrap();
        match kind {
            "public" => {
                std::fs::write(&path, b"unchanged").unwrap();
                std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
            }
            "symlink" => symlink(&target, &path).unwrap(),
            "hardlink" => std::fs::hard_link(&target, &path).unwrap(),
            "directory" => std::fs::create_dir(&path).unwrap(),
            _ => unreachable!(),
        }
        persist
            .observed_command_result(
                &id,
                command,
                &json!({"delivered":true}),
                "stdin_written",
                None,
                vec![],
            )
            .await
            .expect_err("an unsafe log must refuse before send");
        assert_eq!(
            persist.unresolved_recoveries(),
            1,
            "{kind}: observed payload retained"
        );
        let state: String = persist
            .conn
            .query_row("SELECT state FROM commands WHERE id=?", [command], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(state, "claimed", "{kind}: no evidence operation was sent");
        assert_eq!(std::fs::read(&target).unwrap(), b"unchanged");
        if kind == "directory" {
            std::fs::remove_dir(&path).unwrap();
        } else {
            std::fs::remove_file(&path).unwrap();
        }
        assert_eq!(persist.drain_recoveries().await.unwrap(), 1);
        assert_eq!(persist.drain_recoveries().await.unwrap(), 0);
        let receipts: i64 = persist
            .conn
            .query_row(
                "SELECT COUNT(*) FROM operation_receipts WHERE op_kind=?",
                [agent_run_store::ops::OpKindRef::ObservedCommandResult.name()],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(receipts, 1, "{kind}: evidence submitted exactly once");
    }
}

/// An original first built by recovery keeps that exact identity after a
/// durable rejection. Another drain must use row41 unverified recovery rather
/// than rebuilding the original operation and receiving a second receipt.
#[tokio::test]
async fn a_refused_original_built_during_drain_keeps_its_identity() {
    let (home, _harness, mut persist, id, command) = claimed().await;
    persist.bind_attempt("no-such-attempt");
    persist
        .event(&id, "bad_preflush", &json!({}))
        .await
        .unwrap();
    persist.bind_attempt(&format!("{id}:1"));
    persist
        .observed_command_result(
            &id,
            command,
            &json!({"delivered":true}),
            "stdin_written",
            None,
            vec![JournalFragment::Event {
                kind: "bad_evidence".into(),
                data: json!({}),
                attempt: Some("no-such-attempt".into()),
            }],
        )
        .await
        .expect_err("preflush refuses before building the original");
    assert!(
        !log_lines(&home, &id)
            .iter()
            .any(|l| l["line"] == "identity")
    );
    assert_eq!(persist.drain_recoveries().await.unwrap(), 0);
    assert_eq!(persist.unresolved_recoveries(), 1);
    assert_eq!(persist.drain_recoveries().await.unwrap(), 1);
    assert_eq!(persist.unresolved_recoveries(), 0);
    assert_eq!(persist.drain_recoveries().await.unwrap(), 0);
    assert_unverified_only(&home, &persist, &id, command, "bad_evidence");
    let identities = log_lines(&home, &id)
        .into_iter()
        .filter(|l| l["line"] == "identity")
        .count();
    assert_eq!(
        identities, 1,
        "the original was built exactly once across drains"
    );
    let receipts: i64 = persist
        .conn
        .query_row(
            "SELECT COUNT(*) FROM operation_receipts WHERE op_kind=?",
            [agent_run_store::ops::OpKindRef::ObservedCommandResult.name()],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        receipts, 1,
        "the rejected original was not rebuilt with a fresh id"
    );
}
