//! Typed Bridge outbox lease, retry, cancellation and immutable-evidence controls.
use super::*;
use agent_run_store::ops::{
    ClaimDelivery, DeliveryCompletion, DeliveryPolicySpec, PrepareBridgeDelivery,
};

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
