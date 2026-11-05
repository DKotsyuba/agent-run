//! Typed Bridge outbox lease, retry, cancellation and immutable-evidence controls.
use super::*;
use agent_run_store::ops::{
    ClaimDelivery, DeliveryCompletion, DeliveryPolicySpec, PrepareBridgeDelivery,
};
use rusqlite::params;

/// Capture one fixed deterministic policy for these local-store tests, with a ten-second lease.
fn policy() -> DeliveryPolicySpec {
    DeliveryPolicySpec {
        retry_base_seconds: 1.0,
        retry_cap_seconds: 300.0,
        max_attempts: 0,
        lease_seconds: 10.0,
    }
}

/// Claim through the sole supported owner-selection operation, without filesystem or network access.
fn claim(store: &mut agent_run_store::Store, owner: &str, at: f64) -> Option<Value> {
    store
        .claim_delivery_lease(&ClaimDelivery {
            owner: owner.into(),
            at,
            policy: policy(),
        })
        .unwrap()
}

/// Build a known-unsent Bridge verdict; tests alter only the facts whose boundaries they exercise.
fn verdict(id: &str, owner: &str, attempt: u32, at: f64) -> DeliveryCompletion {
    DeliveryCompletion {
        delivery_id: id.into(),
        owner: owner.into(),
        attempt,
        at,
        transport: "bridge".into(),
        accepted: false,
        ambiguous: false,
        classifier: "bridge_unavailable".into(),
        evidence: None,
        bridge_receipt: None,
        policy: policy(),
    }
}

/// Add one already-bound maintenance event and its outbox row to a fixture.
fn maintenance_delivery(
    store: &agent_run_store::Store,
    agent: &agent_run_domain::domain::AgentId,
    event_agent: &agent_run_domain::domain::AgentId,
    session: &str,
    delivery: &str,
    epoch: i64,
    raw: &str,
) {
    store
        .conn
        .execute(
            "INSERT INTO events(agent_id,at,kind,data_json) VALUES(?,4,'maintenance_notice_v1',?)",
            params![event_agent.as_str(), raw],
        )
        .unwrap();
    let event = store.conn.last_insert_rowid();
    store.conn.execute(
        "INSERT INTO deliveries(id,agent_id,orchestrator_session_id,terminal_event_seq,state,next_attempt_at) \
         VALUES(?,?,?,?,'pending',0)",
        params![delivery,agent.as_str(),session,event],
    ).unwrap();
    store.conn.execute(
        "INSERT INTO maintenance_fences(epoch,request_id,created_at,ended_at,member_count,phase,outcome) VALUES(?,?,1,2,1,'complete','deferred')",
        params![epoch, format!("fixture-{delivery}")],
    ).unwrap();
    store.conn.execute(
        "INSERT INTO maintenance_cohort(epoch,agent_id,status,sequence,orchestrator_session_id,attempts_json) VALUES(?,?,'running',1,?,'[]')",
        params![epoch, agent.as_str(), session],
    ).unwrap();
    let followup = serde_json::from_str::<Value>(raw)
        .ok()
        .and_then(|value| value["stage"].as_str().map(str::to_owned))
        .is_some_and(|stage| matches!(stage.as_str(), "after" | "deferred"));
    let before = if followup {
        format!("{delivery}-before")
    } else {
        delivery.to_owned()
    };
    if followup {
        store.conn.execute(
            "INSERT INTO events(agent_id,at,kind,data_json) VALUES(?,3,'maintenance_notice_v1',?)",
            params![agent.as_str(), json!({"maintenance_epoch":epoch,"stage":"pre"}).to_string()],
        ).unwrap();
        let before_event = store.conn.last_insert_rowid();
        store.conn.execute(
            "INSERT INTO deliveries(id,agent_id,orchestrator_session_id,terminal_event_seq,state) VALUES(?,?,?,?,'delivered')",
            params![before, agent.as_str(), session, before_event],
        ).unwrap();
        store.conn.execute(
            "INSERT INTO bridge_delivery_intents(delivery_id,intent_json,custody_state) VALUES(?,'{}','custody_accepted')",
            [&before],
        ).unwrap();
    }
    store.conn.execute(
        "INSERT INTO maintenance_recipients(epoch,orchestrator_session_id,transport,external_session_id,anchor_agent_id,before_delivery_id,after_delivery_id) \
         SELECT ?,id,transport,external_session_id,?,?,? FROM orchestrator_sessions WHERE id=?",
        params![epoch, agent.as_str(), before, followup.then_some(delivery), session],
    ).unwrap();
}

/// The maintenance claim accepts only exact bounded event shapes and quarantines
/// invalid or foreign payloads without ever attempting Bridge publication.
#[test]
fn maintenance_notice_claim_rejects_invalid_and_foreign_events() {
    let home = common::Home::new();
    let agent = create(&home);
    let other = create(&home);
    let mut store = home.store();
    store.running(&agent, 42).unwrap();
    let session = store
        .bind_orchestrator(
            &agent,
            &OrchestratorRef {
                transport: "codex_queue".into(),
                external_session_id: "maintenance-fixture".into(),
                external_turn_id: None,
            },
            1.0,
        )
        .unwrap();
    let cases: Vec<(String, bool)> = vec![
        (json!({"maintenance_epoch":1,"stage":"pre"}).to_string(),true),
        (json!({"maintenance_epoch":1,"stage":"deferred"}).to_string(),true),
        (json!({"maintenance_epoch":i64::MAX,"stage":"after","resumed":u32::MAX,"completed":0,"cancelled":0,"held":0}).to_string(),true),
        (json!({"maintenance_epoch":0,"stage":"pre"}).to_string(),false),
        (json!({"maintenance_epoch":-1,"stage":"pre"}).to_string(),false),
        (json!({"maintenance_epoch":1,"stage":"unknown"}).to_string(),false),
        (json!({"maintenance_epoch":1,"stage":"pre","text":"injected"}).to_string(),false),
        (json!({"maintenance_epoch":1,"stage":"deferred","held":1}).to_string(),false),
        (json!({"maintenance_epoch":1,"stage":"after","resumed":1,"completed":0,"cancelled":0}).to_string(),false),
        (json!({"maintenance_epoch":1,"stage":"after","resumed":u64::from(u32::MAX)+1,"completed":0,"cancelled":0,"held":0}).to_string(),false),
        (json!({"maintenance_epoch":1,"stage":"after","resumed":-1,"completed":0,"cancelled":0,"held":0}).to_string(),false),
        ("{\"maintenance_epoch\":1,\"stage\":\"pre\",\"stage\":\"pre\"}".into(),false),
        (format!("{}{}", " ".repeat(512), json!({"maintenance_epoch":1,"stage":"pre"})),false),
        ("{broken".into(),false),
    ];
    for (index, (raw, valid)) in cases.into_iter().enumerate() {
        let delivery = format!("ntf_maintenance_{index}");
        let epoch = if index == 2 {
            i64::MAX
        } else {
            index as i64 + 1
        };
        let raw = if valid {
            let mut data: Value = serde_json::from_str(&raw).unwrap();
            data["maintenance_epoch"] = json!(epoch);
            data.to_string()
        } else {
            raw
        };
        maintenance_delivery(&store, &agent, &agent, &session, &delivery, epoch, &raw);
        let result = claim(&mut store, "owner", 5.0);
        if valid {
            let result = result.expect("valid event must be claimable");
            assert_eq!(result["delivery_id"], delivery);
            assert_eq!(result["payload"]["kind"], "maintenance");
            assert_eq!(
                result["payload"]["notice"],
                serde_json::from_str::<Value>(&raw).unwrap()
            );
        } else {
            assert!(result.is_none());
            let stored: (String, String) = store
                .conn
                .query_row(
                    "SELECT state,last_error FROM deliveries WHERE id=?",
                    [&delivery],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .unwrap();
            assert_eq!(stored, ("failed".into(), "invalid_payload".into()));
        }
    }
    maintenance_delivery(
        &store,
        &agent,
        &other,
        &session,
        "ntf_foreign",
        100,
        &json!({"maintenance_epoch":1,"stage":"pre"}).to_string(),
    );
    assert!(claim(&mut store, "owner", 5.0).is_none());
    assert_eq!(
        store
            .conn
            .query_row(
                "SELECT last_error FROM deliveries WHERE id='ntf_foreign'",
                [],
                |r| r.get::<_, String>(0)
            )
            .unwrap(),
        "invalid_payload"
    );
    maintenance_delivery(
        &store,
        &agent,
        &agent,
        &session,
        "ntf_wrong_epoch",
        101,
        &json!({"maintenance_epoch":102,"stage":"pre"}).to_string(),
    );
    assert!(claim(&mut store, "owner", 5.0).is_none());
    assert_eq!(
        store
            .conn
            .query_row(
                "SELECT last_error FROM deliveries WHERE id='ntf_wrong_epoch'",
                [],
                |r| r.get::<_, String>(0)
            )
            .unwrap(),
        "invalid_payload"
    );
    maintenance_delivery(
        &store,
        &agent,
        &agent,
        &session,
        "ntf_missing_route",
        102,
        &json!({"maintenance_epoch":102,"stage":"pre"}).to_string(),
    );
    store
        .conn
        .execute(
            "DELETE FROM maintenance_recipients WHERE before_delivery_id='ntf_missing_route'",
            [],
        )
        .unwrap();
    assert!(claim(&mut store, "owner", 5.0).is_none());
    assert_eq!(
        store
            .conn
            .query_row(
                "SELECT last_error FROM deliveries WHERE id='ntf_missing_route'",
                [],
                |r| r.get::<_, String>(0)
            )
            .unwrap(),
        "maintenance_route_invalid"
    );
}

/// Maintenance notices retain the same immutable Bridge identity and unknown
/// custody across lease retry; matching custody is not native delivery proof.
#[test]
fn maintenance_notice_reuses_frozen_intent_after_unknown_custody() {
    use agent_run_domain::bridge::{BridgeIntent, BridgeOwner};
    let home = common::Home::new();
    let agent = create(&home);
    let mut store = home.store();
    store.running(&agent, 42).unwrap();
    let session = store
        .bind_orchestrator(
            &agent,
            &OrchestratorRef {
                transport: "codex_queue".into(),
                external_session_id: "maintenance-fixture".into(),
                external_turn_id: None,
            },
            1.0,
        )
        .unwrap();
    let delivery = "ntf_maintenance";
    maintenance_delivery(
        &store,
        &agent,
        &agent,
        &session,
        delivery,
        1,
        &json!({"maintenance_epoch":1,"stage":"pre"}).to_string(),
    );
    let first = claim(&mut store, "owner", 5.0).unwrap();
    assert_eq!(first["payload"]["kind"], "maintenance");
    let intent = BridgeIntent {
        owner: BridgeOwner {
            registration_socket: "/fixture/registration.sock".into(),
            core_socket: "/fixture/core.sock".into(),
            token_file: "/fixture/token".into(),
            credential_sha256: "a".repeat(64),
            producer: "agent-run".into(),
        },
        request_id: "maintenance-request".into(),
        event_id: "maintenance-event".into(),
        address: "local/codex/fixture/session".into(),
        kind: "agent-run.maintenance".into(),
        message: "fixture".into(),
        predecessor: None,
        intent_hash: "b".repeat(64),
    };
    let op = PrepareBridgeDelivery {
        delivery_id: delivery.into(),
        owner: "owner".into(),
        attempt: 1,
        intent: intent.clone(),
        at: 6.0,
    };
    assert_eq!(store.prepare_bridge_delivery(&op).unwrap(), intent);
    assert_eq!(store.prepare_bridge_delivery(&op).unwrap(), intent);
    let mut changed = op.clone();
    changed.intent.message = "different".into();
    assert!(store.prepare_bridge_delivery(&changed).is_err());
    let mut unknown = verdict(delivery, "owner", 1, 6.0);
    unknown.ambiguous = true;
    unknown.classifier = "bridge_outcome_unknown".into();
    assert_eq!(
        store.complete_delivery_attempt(&unknown).unwrap(),
        "retry_wait"
    );
    let second = claim(&mut store, "new-owner", 7.0).unwrap();
    assert_eq!(second["delivery_id"], delivery);
    assert_eq!(second["attempt"], 2);
    assert_eq!(second["bridge_unknown"], true);
    assert_eq!(
        second["bridge_intent"],
        serde_json::to_value(&intent).unwrap()
    );
    let mut accepted = verdict(delivery, "new-owner", 2, 8.0);
    accepted.accepted = true;
    accepted.classifier = "bridge_custody_accepted".into();
    accepted.bridge_receipt = Some(
        json!({"event_id":intent.event_id,"producer":intent.owner.producer,"intent_hash":intent.intent_hash,"destination":{"address":intent.address},"acceptance_seq":1}),
    );
    assert_eq!(
        store.complete_delivery_attempt(&accepted).unwrap(),
        "delivered"
    );
    assert_eq!(
        agent_run_store::delivery::public_state_on(&store.conn, delivery, "delivered").unwrap(),
        "custody_accepted"
    );
    assert!(claim(&mut store, "owner", 9.0).is_none());
}

/// A follow-up never overtakes an unknown pre-notice, and the blocked row does
/// not stop an unrelated due notification from being claimed.
#[test]
fn maintenance_followup_waits_for_prior_custody_without_blocking_other_notices() {
    let home = common::Home::new();
    let agent = create(&home);
    let mut store = home.store();
    store.running(&agent, 42).unwrap();
    let session = store
        .bind_orchestrator(
            &agent,
            &OrchestratorRef {
                transport: "codex_queue".into(),
                external_session_id: "ordering-fixture".into(),
                external_turn_id: None,
            },
            1.0,
        )
        .unwrap();
    maintenance_delivery(&store, &agent, &agent, &session, "ntf_a_after", 1,
        &json!({"maintenance_epoch":1,"stage":"after","resumed":1,"completed":0,"cancelled":0,"held":0}).to_string());
    store.conn.execute("UPDATE bridge_delivery_intents SET custody_state='outcome_unknown' WHERE delivery_id='ntf_a_after-before'", []).unwrap();
    store.conn.execute("UPDATE deliveries SET state='retry_wait',next_attempt_at=100 WHERE id='ntf_a_after-before'", []).unwrap();
    maintenance_delivery(
        &store,
        &agent,
        &agent,
        &session,
        "ntf_z_pre",
        2,
        &json!({"maintenance_epoch":2,"stage":"pre"}).to_string(),
    );
    assert_eq!(
        claim(&mut store, "owner", 5.0).unwrap()["delivery_id"],
        "ntf_z_pre"
    );
    assert!(claim(&mut store, "owner", 5.0).is_none());
    store.conn.execute("UPDATE bridge_delivery_intents SET custody_state='custody_accepted' WHERE delivery_id='ntf_a_after-before'", []).unwrap();
    store.conn.execute("UPDATE deliveries SET state='delivered',next_attempt_at=NULL WHERE id='ntf_a_after-before'", []).unwrap();
    assert_eq!(
        claim(&mut store, "owner", 5.0).unwrap()["delivery_id"],
        "ntf_a_after"
    );
}

/// Freeze synthetic intent under a live lease and return its matching custody verdict.
/// This proves store matching/atomicity only, not SDK hashing or native visibility.
fn custody(store: &mut agent_run_store::Store, mut done: DeliveryCompletion) -> DeliveryCompletion {
    use agent_run_domain::bridge::{BridgeIntent, BridgeOwner};
    let intent = BridgeIntent {
        owner: BridgeOwner {
            registration_socket: "/fixture/native.sock".into(),
            core_socket: "/fixture/core.sock".into(),
            token_file: "/fixture/token".into(),
            credential_sha256: "a".repeat(64),
            producer: "agent-run".into(),
        },
        request_id: "synthetic-request".into(),
        event_id: "synthetic-event".into(),
        address: "local/codex/fixture/session".into(),
        kind: "agent-run.completion".into(),
        message: "synthetic store test".into(),
        predecessor: None,
        intent_hash: "b".repeat(64),
    };
    store
        .prepare_bridge_delivery(&PrepareBridgeDelivery {
            delivery_id: done.delivery_id.clone(),
            owner: done.owner.clone(),
            attempt: done.attempt,
            intent: intent.clone(),
            at: done.at,
        })
        .unwrap();
    done.accepted = true;
    done.classifier = "bridge_custody_accepted".into();
    done.bridge_receipt = Some(
        json!({"event_id":intent.event_id,"producer":intent.owner.producer,
        "intent_hash":intent.intent_hash,"destination":{"address":intent.address},"acceptance_seq":1}),
    );
    done
}

/// Expired and foreign lease owners cannot finalize; one exact reclaimer can commit matching custody.
#[test]
fn expired_lease_reclaims_once_and_rejects_stale_ack() {
    let home = common::Home::new();
    let (_, delivery) = bound_delivery(&home);
    let mut store = home.store();
    let first = claim(&mut store, "one", 5.0).unwrap();
    assert_eq!(
        (
            first["delivery_id"].as_str().unwrap(),
            first["attempt"].as_u64().unwrap()
        ),
        (delivery.as_str(), 1)
    );
    assert!(claim(&mut store, "two", 14.0).is_none());
    assert_eq!(
        store
            .complete_delivery_attempt(&verdict(&delivery, "one", 1, 15.0))
            .unwrap(),
        "not_owned"
    );
    let reclaimed = claim(&mut store, "two", 15.0).unwrap();
    assert_eq!(reclaimed["attempt"], 2);
    assert!(claim(&mut store, "three", 15.0).is_none());
    assert_eq!(
        store
            .complete_delivery_attempt(&verdict(&delivery, "one", 1, 16.0))
            .unwrap(),
        "not_owned"
    );
    let done = custody(&mut store, verdict(&delivery, "two", 2, 16.0));
    assert_eq!(store.complete_delivery_attempt(&done).unwrap(), "delivered");
    assert_eq!(
        agent_run_store::delivery::public_state_on(&store.conn, &delivery, "delivered").unwrap(),
        "custody_accepted"
    );
    assert_eq!(reclaimed["payload"]["notice"]["status"], "succeeded");
}

/// Retry backoff/cancellation preserves successful domain outcome and immutable attempt evidence.
#[test]
fn retry_cancel_and_evidence_are_independent_of_domain_completion() {
    let home = common::Home::new();
    let (id, delivery) = bound_delivery(&home);
    let mut store = home.store();
    claim(&mut store, "owner", 5.0).unwrap();
    let mut done = verdict(&delivery, "owner", 1, 6.0);
    done.evidence = Some(evidence("bridge_unavailable", 4));
    assert_eq!(
        store.complete_delivery_attempt(&done).unwrap(),
        "retry_wait"
    );
    assert_eq!(
        store
            .conn
            .query_row("SELECT next_attempt_at FROM deliveries", [], |r| r
                .get::<_, f64>(0))
            .unwrap(),
        7.0
    );
    assert!(claim(&mut store, "owner", 6.9).is_none());
    assert_eq!(claim(&mut store, "owner", 7.0).unwrap()["attempt"], 2);
    assert_eq!(
        store.latest_delivery_attempt(&delivery).unwrap().unwrap()["duration_ms"],
        4
    );
    assert!(store.cancel_delivery(&delivery).unwrap());
    assert_eq!(
        store
            .complete_delivery_attempt(&verdict(&delivery, "owner", 2, 8.0))
            .unwrap(),
        "not_owned"
    );
    assert!(claim(&mut store, "other", 1000.0).is_none());
    assert_eq!(store.get(&id).unwrap().status.as_str(), "succeeded");
    assert_eq!(
        store
            .conn
            .query_row("SELECT COUNT(*) FROM delivery_attempt_evidence", [], |r| {
                r.get::<_, i64>(0)
            })
            .unwrap(),
        1
    );
}

/// Native verdicts and mismatched custody are rejected without changing the leased row or evidence.
#[test]
fn removed_native_verdict_and_foreign_receipt_cannot_release_outbox() {
    let home = common::Home::new();
    let (_, delivery) = bound_delivery(&home);
    let mut store = home.store();
    claim(&mut store, "owner", 5.0).unwrap();
    let mut done = verdict(&delivery, "owner", 1, 6.0);
    done.transport = "codex_queue".into();
    done.accepted = true;
    assert!(store.complete_delivery_attempt(&done).is_err());
    let mut done = custody(&mut store, verdict(&delivery, "owner", 1, 6.0));
    done.evidence = Some(evidence("bridge_custody_accepted", 1));
    done.bridge_receipt.as_mut().unwrap()["producer"] = json!("other");
    assert!(store.complete_delivery_attempt(&done).is_err());
    assert_eq!(
        store
            .conn
            .query_row("SELECT state FROM deliveries", [], |r| r
                .get::<_, String>(0))
            .unwrap(),
        "sending"
    );
    assert_eq!(
        store
            .conn
            .query_row("SELECT COUNT(*) FROM delivery_attempt_evidence", [], |r| {
                r.get::<_, i64>(0)
            })
            .unwrap(),
        0
    );
    done.bridge_receipt.as_mut().unwrap()["producer"] = json!("agent-run");
    assert_eq!(store.complete_delivery_attempt(&done).unwrap(), "delivered");
    assert_eq!(
        store.latest_delivery_attempt(&delivery).unwrap().unwrap()["classifier"],
        "bridge_custody_accepted"
    );
}
