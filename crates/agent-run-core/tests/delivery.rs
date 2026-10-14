//! Completion delivery parity fixtures for Python's delivery dispatcher and notice contract.

use agent_run_core::{
    delivery::{
        CompletionNotice, NOTICE_VERSION, Notice, dispatch as real_dispatch,
        dispatch_once as real_dispatch_once, dispatch_with_batch as real_dispatch_with_batch,
        safe_evidence,
    },
    domain::{AgentId, Status, now},
};
use rusqlite::{Connection, params};
use serde_json::{Value, json};
use std::{
    path::{Path, PathBuf},
    time::Duration,
};

mod common;

/// Synthetic authenticated delivery-only capability response; no native qualification.
fn bridge_capabilities(allowed: bool) -> bridge_sdk::bridge_protocol::Capabilities {
    use bridge_sdk::bridge_protocol::*;
    Capabilities {
        api_version: 1,
        supported_versions: vec![1],
        operations: vec!["capabilities".into(), "publish".into(), "status".into()],
        evidence_variants: vec![],
        native_contracts: vec![],
        modes: vec![Mode::Message],
        policies: vec![PolicyKind::KeepAll],
        reply_policies: vec![ReplyPolicy::SelfActor],
        limits: Limits {
            registration_request_bytes: 524288,
            core_request_bytes: 524288,
            response_bytes: 524288,
            payload_bytes: 65536,
            metadata_entries: 16,
            rendered_manifest_bytes: 98304,
            open_routes: 0,
            assertions: 0,
        },
        grant: ProducerConstraints {
            allowed_modes: vec![Mode::Message],
            allowed_policies: vec![PolicyKind::KeepAll],
            allowed_kinds: vec!["*".into()],
            reply_policies: vec![ReplyPolicy::SelfActor],
            direct_address: allowed,
            native_scopes: vec![],
        },
        features: Features {
            consume_assertion: false,
            delivery_scopes: false,
            host_observations: false,
        },
        retention: Retention {
            durable_keys: true,
            closed_tombstones: true,
            credential_independent_identity: true,
        },
    }
}

/// Exact synthetic custody for one frozen address publication, with no native acceptance.
fn bridge_receipt(
    params: &bridge_sdk::bridge_protocol::PublishParams,
) -> bridge_sdk::bridge_protocol::EventReceipt {
    use bridge_sdk::bridge_protocol::*;
    let Destination::Address { address } = &params.destination else {
        panic!("delivery-only address expected")
    };
    EventReceipt {
        producer: "agent-run".into(),
        event_id: params.event_id.clone(),
        intent_hash: publication_intent_fingerprint(params).unwrap(),
        envelope_hash: "a".repeat(64),
        accepted_at_ms: 10,
        acceptance_seq: 1,
        destination: FrozenDestination {
            address: address.clone(),
            route_key: None,
            caller_scope: None,
            destination_scope: None,
        },
    }
}

/// Owner-authorized opt-in native control through the actual outbox/SDK adapter.
/// Uses a fresh disposable home, an explicit protected config and recipient;
/// retains only private intent/receipt artifacts, launches no provider engine,
/// and never reads or changes a production Agent Run home. Ordinary CI skips it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires an owner-authorized temporary Bridge grant and independent native receiver ACK"]
async fn bridge_live_outbox_probe() {
    use agent_run_config::config::BridgeDelivery;
    use bridge_sdk::bridge_protocol::{Host, NativeAddress};
    let config_path = std::env::var_os("AGENT_RUN_BRIDGE_LIVE_CONFIG")
        .expect("explicit protected probe configuration required");
    let config: BridgeDelivery =
        toml::from_str(&std::fs::read_to_string(config_path).unwrap()).unwrap();
    config.validate().unwrap();
    let address = NativeAddress::parse(
        &std::env::var("AGENT_RUN_BRIDGE_LIVE_RECIPIENT")
            .expect("explicit native test recipient required"),
    )
    .unwrap();
    assert_eq!(address.host, config.host_id);
    let (transport, store) = match address.harness {
        Host::Codex => ("codex_queue", config.codex_store.as_ref()),
        Host::ClaudeCode => ("claude_uds", config.claude_store.as_ref()),
    };
    assert_eq!(store, Some(&address.store));
    let home = common::Home::new();
    let marker = format!("ARBRIDGE_LIVE_{}", uuid::Uuid::new_v4().simple());
    let completion = format!("ntf_{marker}_completion");
    delivery(&home.path, &completion, transport, "pending");
    let original = std::fs::read_to_string(home.path.join("config.toml")).unwrap();
    std::fs::write(
        home.path.join("config.toml"),
        format!(
            "{original}\n[delivery.bridge]\n{}",
            toml::to_string(&config).unwrap()
        ),
    )
    .unwrap();
    {
        let connection = fixture_connection(home.path.join("state.db")).unwrap();
        connection
            .execute(
                "UPDATE orchestrator_sessions SET external_session_id=? WHERE id='sess'",
                [&address.session],
            )
            .unwrap();
        connection.execute("UPDATE agents SET runtime='synthetic-bridge-probe',model='control',task='synthetic transport control'", []).unwrap();
        let agent = "ag-20260825-120000-0123456789";
        connection.execute("INSERT INTO attempts(id,agent_id,number,state,adapter_state_json,created_at) VALUES('probe-attempt',?,1,'finished','{}',?)", params![agent, now()]).unwrap();
        let worker = format!("ntf_{marker}_worker");
        connection.execute("INSERT INTO deliveries(id,agent_id,orchestrator_session_id,state,next_attempt_at) VALUES(?,?,'sess','pending',?)", params![worker, agent, now() - 1.0]).unwrap();
        connection.execute("INSERT INTO worker_notifications(delivery_id,agent_id,attempt_id,request_id,kind,message,created_at) VALUES(?,?,'probe-attempt',?,'notice',?,?)", params![worker, agent, marker, format!("Synthetic native delivery control {marker}; ACK only, no task approval."), now()]).unwrap();
        let pool_id = format!(
            "pool-20261013-000000-{}",
            &uuid::Uuid::new_v4().simple().to_string()[..10]
        );
        for (suffix, event_kind) in [
            ("pool", "pool_completed"),
            ("attention", "pool_join_needs_action"),
        ] {
            let id = format!("ntf_{marker}_{suffix}");
            connection.execute("INSERT INTO events(agent_id,at,kind,data_json) VALUES(?,?,?,?)", params![agent, now(), event_kind,
                json!({"pool_id":pool_id,"notice":format!("Synthetic native delivery control {marker}_{suffix}; ACK only, no task approval.")}).to_string()]).unwrap();
            let event = connection.last_insert_rowid();
            connection.execute("INSERT INTO deliveries(id,agent_id,orchestrator_session_id,terminal_event_seq,state,next_attempt_at) VALUES(?,?,'sess',?,'pending',?)", params![id, agent, event, now() - 1.0]).unwrap();
            if suffix == "pool" {
                connection.execute("INSERT INTO pools(id,request_namespace,request_id,request_sha256,orchestrator_session_id,goal,acceptance_json,state,roster_revision,completed_at,completion_delivery_id,created_at) VALUES(?,'synthetic-probe',?,?,'sess','synthetic transport control','[]','completed',1,?,?,?)", params![pool_id, marker, "a".repeat(64), now(), id, now()]).unwrap();
            }
        }
    }
    let result = dispatch_with_batch(&home.path, 4).await.unwrap();
    assert_eq!(
        result.transferred, 4,
        "all four probe intentions need exact custody"
    );
    assert_eq!(
        result.delivered, 0,
        "custody must not invent native receipt"
    );
    let connection = fixture_connection(home.path.join("state.db")).unwrap();
    let mut statement = connection.prepare("SELECT intent_json,custody_receipt_json FROM bridge_delivery_intents ORDER BY delivery_id").unwrap();
    let facts: Vec<Value> = statement.query_map([], |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))).unwrap()
        .map(|row| { let (intent, receipt) = row.unwrap(); let intent: agent_run_domain::bridge::BridgeIntent = serde_json::from_str(&intent).unwrap();
            let receipt: Value = serde_json::from_str(&receipt).unwrap(); json!({"event_id":intent.event_id,"kind":intent.kind,"intent_hash":intent.intent_hash,"receipt":receipt}) }).collect();
    std::fs::write(
        home.path.join("live-custody.json"),
        serde_json::to_vec_pretty(&json!({"marker":marker,"events":facts})).unwrap(),
    )
    .unwrap();
    assert_eq!(
        dispatch_with_batch(&home.path, 4).await.unwrap().claimed,
        0,
        "released intents cannot publish again"
    );
    // Fixture-only acknowledgement-loss injection: real recovery must read the
    // same four authoritative custody receipts without another publication.
    connection.execute("UPDATE bridge_delivery_intents SET custody_state='outcome_unknown',custody_receipt_json=NULL", []).unwrap();
    connection.execute("UPDATE deliveries SET state='retry_wait',next_attempt_at=?,lease_owner=NULL,lease_until=NULL", [now() - 1.0]).unwrap();
    let recovered = dispatch_with_batch(&home.path, 4).await.unwrap();
    assert_eq!(recovered.transferred, 4);
    assert_eq!(recovered.delivered, 0);
    assert_eq!(dispatch_with_batch(&home.path, 4).await.unwrap().claimed, 0);
    std::fs::write(
        home.path.join("live-recovery.json"),
        serde_json::to_vec_pretty(&json!({"same_events_recovered":4,"new_publications":0}))
            .unwrap(),
    )
    .unwrap();
    drop(statement);
    drop(connection);
    let retained = home.temp.keep();
    println!("probe sender retained at {}", retained.display());
}

/// Lost custody acknowledgements survive a refusal and configuration change,
/// then recover the identical event. Local custody is never native delivery.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bridge_delivery_lost_ack_keeps_single_owner_and_frozen_intent() {
    use bridge_sdk::bridge_protocol::*;
    use std::{
        os::unix::fs::PermissionsExt,
        sync::{
            Arc, Mutex,
            atomic::{AtomicBool, AtomicU8, Ordering},
        },
    };
    use tokio::{
        io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
        net::UnixListener,
    };
    let home = common::Home::new();
    let socket = home.path.join("bridge.sock");
    // Even one allowed publication failure cannot terminate unknown-custody reconciliation.
    delivery_config(&home.path, 1.0, 1.0, 1);
    let listener = UnixListener::bind(&socket).unwrap();
    let token = home.path.join("bridge-token");
    std::fs::write(&token, "synthetic-only-producer-credential").unwrap();
    std::fs::set_permissions(&token, std::fs::Permissions::from_mode(0o600)).unwrap();
    let configuration = || {
        format!(
            "{}\n[delivery.bridge]\nregistration_socket={:?}\ncore_socket={:?}\ntoken_file={:?}\nhost_id='local'\ncodex_store='codex-main'\n",
            std::fs::read_to_string(home.path.join("config.toml")).unwrap(),
            socket,
            socket,
            token
        )
    };
    let configured = configuration();
    std::fs::write(home.path.join("config.toml"), &configured).unwrap();
    delivery(&home.path, "ntf_bridge", "codex_queue", "pending");
    let publications = Arc::new(Mutex::new(Vec::<(String, PublishParams)>::new()));
    let allowed = Arc::new(AtomicBool::new(true));
    let visible = Arc::new(AtomicBool::new(true));
    let corrupt_receipt = Arc::new(AtomicU8::new(0));
    let observed = publications.clone();
    let grant = allowed.clone();
    let history = visible.clone();
    let corruption = corrupt_receipt.clone();
    let service = tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            let mut reader = BufReader::new(stream);
            let mut line = String::new();
            reader.read_line(&mut line).await.unwrap();
            let request = decode_request(line.trim_end().as_bytes()).unwrap();
            let result = match &request.operation {
                Operation::Capabilities(_) => {
                    OperationResult::Capabilities(bridge_capabilities(grant.load(Ordering::SeqCst)))
                }
                Operation::Publish(params) => {
                    let attempts = {
                        let mut stored = observed.lock().unwrap();
                        if let Some((id, old)) = stored.first() {
                            assert_eq!(id, &request.request_id);
                            assert_eq!(old, params);
                        }
                        stored.push((request.request_id.clone(), params.clone()));
                        stored.len()
                    };
                    if attempts <= 2 {
                        continue;
                    } // Accept once, deliberately lose both SDK acknowledgements.
                    OperationResult::Publish(PublishResult {
                        replayed: true,
                        receipt: bridge_receipt(params),
                    })
                }
                Operation::Status(_) => OperationResult::Status(StatusResult::Event {
                    event: history.load(Ordering::SeqCst).then(|| EventStatus {
                        receipt: {
                            let mut receipt = bridge_receipt(&observed.lock().unwrap()[0].1);
                            match corruption.load(Ordering::SeqCst) {
                                1 => receipt.producer = "other-producer".into(),
                                2 => receipt.intent_hash = "b".repeat(64),
                                _ => {}
                            }
                            receipt
                        },
                        custody_state: CustodyState::Pending,
                        delivery: DeliveryStatus {
                            delivery: DeliveryState::Unhanded,
                            attempt_state: None,
                            effective_mode: None,
                            note: None,
                            attempts_made: 0,
                            attempt: None,
                            tiers: vec![],
                            incident: None,
                            hold_reason: None,
                            blocked_by: None,
                        },
                    }),
                }),
                _ => panic!("fixture received unexpected operation"),
            };
            let mut response = encode_response(&Response::ok(&request.request_id, result)).unwrap();
            response.push(b'\n');
            reader.get_mut().write_all(&response).await.unwrap();
        }
    });
    let first = dispatch_with_batch(&home.path, 1).await.unwrap();
    assert_eq!(first.ambiguous, 1);
    assert_eq!(first.delivered, 0);
    assert_eq!(first.transferred, 0);
    let state = || {
        agent_run_store::delivery::public_state_on(
            &fixture_connection(home.path.join("state.db")).unwrap(),
            "ntf_bridge",
            "retry_wait",
        )
        .unwrap()
    };
    assert_eq!(state(), "outcome_unknown");
    assert_eq!(publications.lock().unwrap().len(), 2);
    // No visible historical record cannot erase possible committed custody or permit republishing.
    allowed.store(false, Ordering::SeqCst);
    visible.store(false, Ordering::SeqCst);
    make_due(&home.path, "ntf_bridge");
    let refused = dispatch_with_batch(&home.path, 1).await.unwrap();
    assert_eq!(refused.ambiguous, 1);
    assert_eq!(state(), "outcome_unknown");
    assert_eq!(publications.lock().unwrap().len(), 2);
    // Another deployment/publisher must not even receive a connection for the old unknown intent.
    let other_socket = home.path.join("other.sock");
    let other_listener = UnixListener::bind(&other_socket).unwrap();
    std::fs::write(
        home.path.join("config.toml"),
        configured.replace(socket.to_str().unwrap(), other_socket.to_str().unwrap()),
    )
    .unwrap();
    make_due(&home.path, "ntf_bridge");
    let held = dispatch_with_batch(&home.path, 1).await.unwrap();
    assert_eq!(held.ambiguous, 1);
    assert_eq!(state(), "outcome_unknown");
    assert!(
        tokio::time::timeout(Duration::from_millis(10), other_listener.accept())
            .await
            .is_err()
    );
    assert_eq!(publications.lock().unwrap().len(), 2);
    // A same-event status with foreign producer or changed intent hash cannot release custody.
    std::fs::write(home.path.join("config.toml"), &configured).unwrap();
    visible.store(true, Ordering::SeqCst);
    for defect in [1, 2] {
        corrupt_receipt.store(defect, Ordering::SeqCst);
        make_due(&home.path, "ntf_bridge");
        let rejected = dispatch_with_batch(&home.path, 1).await.unwrap();
        assert_eq!(rejected.transferred, 0);
        assert_eq!(state(), "outcome_unknown");
        assert_eq!(publications.lock().unwrap().len(), 2);
    }
    corrupt_receipt.store(0, Ordering::SeqCst);
    // Same-owner history survives both new-policy tightening and a trusted same-publisher key rotation.
    std::fs::write(&token, "synthetic-rotated-same-publisher-credential").unwrap();
    std::fs::write(
        home.path.join("config.toml"),
        configured.replace("codex-main", "changed-store"),
    )
    .unwrap();
    make_due(&home.path, "ntf_bridge");
    let restored = dispatch_with_batch(&home.path, 1).await.unwrap();
    assert_eq!(restored.transferred, 1);
    assert_eq!(restored.delivered, 0);
    assert_eq!(state(), "custody_accepted");
    assert_eq!(publications.lock().unwrap().len(), 2);
    assert_eq!(
        publications.lock().unwrap()[0].1.destination,
        Destination::Address {
            address: "local/codex/codex-main/thread".into()
        }
    );
    assert_eq!(dispatch_with_batch(&home.path, 1).await.unwrap().claimed, 0);
    let connection = fixture_connection(home.path.join("state.db")).unwrap();
    let receipt: String = connection.query_row("SELECT custody_receipt_json FROM bridge_delivery_intents WHERE delivery_id='ntf_bridge'", [], |row| row.get(0)).unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(&receipt).unwrap()["acceptance_seq"],
        1
    );
    let diagnostic = connection.query_row("SELECT evidence_json FROM delivery_attempt_evidence WHERE delivery_id='ntf_bridge' ORDER BY attempt DESC LIMIT 1", [], |row| row.get::<_, String>(0)).unwrap();
    assert!(!diagnostic.contains("synthetic-only-producer-credential"));
    assert!(!diagnostic.contains("agent-run/completion"));
    assert!(!diagnostic.contains("codex-main/thread"));
    service.abort();
    let _ = service.await;
}

/// Existing attempted native effects never become a new Bridge publication.
#[tokio::test]
async fn bridge_cutover_retains_attempted_legacy_owner() {
    let home = common::Home::new();
    let text = std::fs::read_to_string(home.path.join("config.toml")).unwrap();
    std::fs::write(home.path.join("config.toml"), format!("{text}\n[delivery.bridge]\nregistration_socket='/not-used/a.sock'\ncore_socket='/not-used/c.sock'\ntoken_file='/not-used/token'\nhost_id='local'\ncodex_store='codex-main'\n")).unwrap();
    delivery(&home.path, "ntf_old", "codex_queue", "retry_wait");
    let connection = fixture_connection(home.path.join("state.db")).unwrap();
    connection
        .execute("UPDATE deliveries SET attempts=1 WHERE id='ntf_old'", [])
        .unwrap();
    let outcome = dispatch_with_batch(&home.path, 1).await.unwrap();
    assert_eq!(outcome.transferred, 0);
    assert_eq!(
        connection
            .query_row("SELECT COUNT(*) FROM bridge_delivery_intents", [], |row| {
                row.get::<_, i64>(0)
            })
            .unwrap(),
        0
    );
    assert_eq!(
        connection
            .query_row(
                "SELECT last_error FROM deliveries WHERE id='ntf_old'",
                [],
                |row| row.get::<_, String>(0)
            )
            .unwrap(),
        "legacy_delivery_held"
    );
}

/// A first-lease crash before payload preparation retains Bridge ownership on
/// both harnesses, even when the current configuration no longer enables Bridge.
#[tokio::test]
async fn bridge_lease_crash_before_prepare_cannot_fall_back_to_native() {
    for transport in ["codex_queue", "claude_uds"] {
        let home = common::Home::new();
        delivery(&home.path, "ntf_crash", transport, "pending");
        let mut store = home.store();
        let claimed = store
            .claim_delivery_lease(&agent_run_store::ops::ClaimDelivery {
                owner: "crashed-owner".into(),
                at: now(),
                policy: agent_run_store::ops::DeliveryPolicySpec {
                    retry_base_seconds: 2.0,
                    retry_cap_seconds: 60.0,
                    max_attempts: 0,
                    lease_seconds: 30.0,
                },
            })
            .unwrap()
            .unwrap();
        assert_eq!(claimed["bridge_owned"], true);
        assert!(claimed["bridge_intent"].is_null());
        store
            .conn
            .execute(
                "UPDATE deliveries SET lease_until=? WHERE id='ntf_crash'",
                [now() - 1.0],
            )
            .unwrap();
        drop(store);
        let dispatched = dispatch_with_batch(&home.path, 1).await.unwrap();
        assert_eq!(dispatched.delivered, 0);
        assert_eq!(dispatched.retried, 1);
        let connection = fixture_connection(home.path.join("state.db")).unwrap();
        assert_eq!(
            connection
                .query_row(
                    "SELECT last_error FROM deliveries WHERE id='ntf_crash'",
                    [],
                    |row| row.get::<_, String>(0)
                )
                .unwrap(),
            "bridge_unconfigured"
        );
        assert_eq!(
            connection
                .query_row("SELECT COUNT(*) FROM bridge_delivery_intents", [], |row| {
                    row.get::<_, i64>(0)
                })
                .unwrap(),
            1
        );
    }
}

/// Returns a repository fixture path without making the immutable fixture writable.
fn fixture(path: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join(path)
}

/// Inserts the minimum terminal agent, bound session, and delivery rows for a dispatcher test.
fn delivery(home: &Path, id: &str, transport: &str, state: &str) {
    let connection = fixture_connection(home.join("state.db")).unwrap();
    connection
        .execute(
            "INSERT INTO orchestrator_sessions(id,transport,external_session_id,created_at,last_seen_at) VALUES('sess',?,?,?,?)",
            params![transport, "thread", now(), now()],
        )
        .unwrap();
    connection
        .execute(
            "INSERT INTO agents(id,runtime,model,profile,task,task_summary,workdir,request_json,status,created_at,config_revision,root_agent_id) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
            params!["ag-20260825-120000-0123456789", "mock", "fixture", "review", "private task", "summary", home.to_string_lossy(), "{}", "succeeded", now(), "fixture", "ag-20260825-120000-0123456789"],
        )
        .unwrap();
    connection
        .execute(
            "INSERT INTO deliveries(id,agent_id,orchestrator_session_id,state,next_attempt_at) VALUES(?,?, 'sess',?,?)",
            params![id, "ag-20260825-120000-0123456789", state, now() - 1.0],
        )
        .unwrap();
}

/// Rewrites only delivery policy values in the fixture configuration.
fn delivery_config(home: &Path, base: f64, cap: f64, max_attempts: u32) {
    let path = home.join("config.toml");
    let mut config = std::fs::read_to_string(&path).unwrap();
    config.push_str(&format!(
        "\n[delivery]\nretry_base_seconds={base}\nretry_cap_seconds={cap}\nmax_attempts={max_attempts}\n"
    ));
    std::fs::write(path, config).unwrap();
}

/// Makes one pending delivery due immediately after an earlier attempt.
fn make_due(home: &Path, id: &str) {
    fixture_connection(home.join("state.db"))
        .unwrap()
        .execute(
            "UPDATE deliveries SET next_attempt_at=? WHERE id=?",
            params![now() - 1.0, id],
        )
        .unwrap();
}

/// Builds one notice from a frozen rendering fixture input object.
fn notice_from_case(input: &Value) -> Notice {
    Notice {
        run_id: None,
        notification_id: input["notification_id"].as_str().unwrap().into(),
        agent_id: input["agent_id"]
            .as_str()
            .unwrap()
            .parse::<AgentId>()
            .unwrap(),
        status: serde_json::from_value(input["status"].clone()).unwrap(),
        runtime: input
            .get("runtime")
            .and_then(Value::as_str)
            .map(str::to_owned),
        model: input
            .get("model")
            .and_then(Value::as_str)
            .map(str::to_owned),
        effort: input
            .get("effort")
            .and_then(Value::as_str)
            .map(str::to_owned),
        failure_kind: input
            .get("failure_kind")
            .and_then(Value::as_str)
            .map(str::to_owned),
    }
}

/// Mirrors `tests/test_delivery_base.py::test_every_terminal_status_renders_its_own_line`.
/// Mirrors `tests/test_delivery_base.py::test_failed_notice_explains_known_and_unknown_failure_categories`.
/// Mirrors `tests/test_delivery_base.py::test_configured_identifier_punctuation_renders_verbatim`.
/// Mirrors `tests/test_delivery_base.py::test_metadata_can_never_add_list_lines_or_commands`.
/// Mirrors `tests/test_delivery_base.py::test_missing_metadata_renders_unknown_and_unspecified`.
/// Replays every frozen Python capture, with only the retired timeout recovery advice
/// projected to the current explicit-resume contract.
#[test]
fn notice_rendering_matches_python_golden_cases() {
    let cases: Vec<Value> = serde_json::from_slice(
        &std::fs::read(fixture("tests/fixtures/baseline/notices/cases.json")).unwrap(),
    )
    .unwrap();
    for case in cases {
        let input = case.get("input").unwrap();
        let notice = notice_from_case(input);
        let expected = case["rendered"].as_str().unwrap().replace(
            "Inspect progress and transcript; increase the timeout only when the run was making useful progress, then retry with a fresh ID.",
            "Inspect progress and transcript; resume explicitly after the native-history, authority and cleanup checks pass. Executions have no time limit.",
        );
        assert_eq!(notice.render().unwrap(), expected, "{}", case["id"]);
    }
}

/// Mirrors `tests/test_codex_queue.py::test_attempt_evidence_is_bounded_redacted_and_exactly_classified`.
#[test]
fn evidence_redacts_secret_shaped_tails_and_caps_utf8() {
    let raw = json!({
        "classifier":"retry", "executable":"codex-queue", "argv_shape":["codex-queue", "<redacted>"],
        "duration_ms":1, "returncode":75, "spawn_errno":null, "error_class":"temporary",
        "stdout_tail": format!("Authorization: Bearer sk-delivery-secret\\n{}", "ёж".repeat(3000)),
        "stderr_tail":"token=also-secret", "stdout_bytes":10000, "stderr_bytes":17,
        "stdout_truncated":false, "stderr_truncated":false, "message_id_present":false
    });
    let safe = safe_evidence(&raw).unwrap();
    let persisted = serde_json::to_string(&safe).unwrap();
    assert!(!persisted.contains("sk-delivery-secret"));
    assert!(!persisted.contains("also-secret"));
    assert!(safe.stdout_tail.len() <= 4096);
    assert!(safe.stderr_tail.len() <= 4096);
    assert!(persisted.len() <= 16 * 1024);
}

/// Cancellation evidence remains stored while the public notice omits failure metadata;
/// two queued notices advance without touching a real transport.
#[tokio::test]
async fn stability_cancelled_notice_does_not_block_outbox() {
    let home = common::Home::new();
    delivery(&home.path, "ntf_first", "fixture-unsupported", "pending");
    let db = fixture_connection(home.path.join("state.db")).unwrap();
    db.execute(
        "UPDATE agents SET status='cancelled',failure_kind='cancelled_before_spawn'",
        [],
    )
    .unwrap();
    db.execute("INSERT INTO deliveries(id,agent_id,orchestrator_session_id,state,next_attempt_at) SELECT 'ntf_next',agent_id,orchestrator_session_id,'pending',next_attempt_at+1 FROM deliveries", []).unwrap();
    assert_eq!(dispatch_once(&home.path).await.unwrap(), 1);
    assert_eq!(dispatch_once(&home.path).await.unwrap(), 1);
    let pending: i64 = db
        .query_row(
            "SELECT COUNT(*) FROM deliveries WHERE state='pending'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(pending, 0);
    let kind: String = db
        .query_row("SELECT failure_kind FROM agents", [], |row| row.get(0))
        .unwrap();
    assert_eq!(kind, "cancelled_before_spawn");
}

/// A malformed stored payload is quarantined once; later due rows keep progressing.
#[tokio::test]
async fn stability_invalid_payload_isolated_from_next_notice() {
    let home = common::Home::new();
    delivery(&home.path, "ntf_first", "fixture-unsupported", "pending");
    let db = fixture_connection(home.path.join("state.db")).unwrap();
    db.execute("UPDATE agents SET root_agent_id='invalid'", [])
        .unwrap();
    db.execute("INSERT INTO agents(id,runtime,model,profile,task,task_summary,workdir,request_json,status,created_at,config_revision,root_agent_id) VALUES('ag-20261004-000000-0000000002','mock','fixture','review','t','t','/tmp','{}','succeeded',1,'fixture','ag-20261004-000000-0000000002')", []).unwrap();
    db.execute("INSERT INTO deliveries(id,agent_id,orchestrator_session_id,state,next_attempt_at) VALUES('ntf_next','ag-20261004-000000-0000000002','sess','pending',?)", [now()-0.5]).unwrap();
    assert_eq!(dispatch_once(&home.path).await.unwrap(), 0);
    let bad: (String, String) = db
        .query_row(
            "SELECT state,last_error FROM deliveries WHERE id='ntf_first'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(bad, ("failed".into(), "invalid_payload".into()));
    assert_eq!(dispatch_once(&home.path).await.unwrap(), 1);
}

/// Mirrors `tests/test_delivery_dispatch.py::test_never_bound_notice_for_a_terminal_agent_expires_after_the_window`.
#[tokio::test]
async fn unbound_terminal_delivery_expires_before_claiming() {
    let home = common::Home::new();
    let connection = fixture_connection(home.path.join("state.db")).unwrap();
    let agent = "ag-20260825-120000-0123456789";
    connection.execute("INSERT INTO agents(id,runtime,model,profile,task,task_summary,workdir,request_json,status,created_at,config_revision,root_agent_id) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)", params![agent,"mock","fixture","review","private task","summary",home.path.to_string_lossy(),"{}","succeeded",now()-7200.0,"fixture",agent]).unwrap();
    connection
        .execute(
            "INSERT INTO events(agent_id,at,kind,data_json) VALUES(?,?,?,?)",
            params![agent, now() - 7200.0, "status", "{}"],
        )
        .unwrap();
    let event: i64 = connection.last_insert_rowid();
    connection.execute("INSERT INTO deliveries(id,agent_id,terminal_event_seq,state) VALUES(?,?,?,'waiting_binding')", params!["ntf_expire",agent,event]).unwrap();
    drop(connection);
    assert_eq!(dispatch_once(&home.path).await.unwrap(), 0);
    let connection = fixture_connection(home.path.join("state.db")).unwrap();
    let state: String = connection
        .query_row(
            "SELECT state FROM deliveries WHERE id='ntf_expire'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(state, "expired");
}

/// Fixture-integrity check: the frozen Python database capture is copied
/// before inspection so this test suite never mutates a shared golden file.
/// Not a direct port of a single Python test.
#[test]
fn golden_delivery_database_is_read_from_a_copy() {
    let temporary = tempfile::tempdir().unwrap();
    let copy = temporary.path().join("current-v16.sqlite");
    std::fs::copy(
        fixture("tests/fixtures/baseline/db/current-v16.sqlite"),
        &copy,
    )
    .unwrap();
    let connection = fixture_connection(copy).unwrap();
    let rows: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM delivery_attempt_evidence",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let state: String = connection
        .query_row(
            "SELECT state FROM deliveries WHERE id='delivery-succeeded'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(rows, 2);
    assert_eq!(state, "delivered");
}

/// Builds a valid notice without any task, answer, or host-provided text.
fn notice() -> Notice {
    Notice {
        run_id: None,
        notification_id: "ntf_test".into(),
        agent_id: "ag-20260825-120000-0123456789".parse().unwrap(),
        status: Status::Succeeded,
        runtime: None,
        model: None,
        effort: None,
        failure_kind: None,
    }
}

/// Mirrors `tests/test_delivery_base.py::CompletionNoticeTests::test_positional_constructors_remain_backward_compatible`
#[test]
fn positional_constructors_remain_backward_compatible() {
    let agent: AgentId = "ag-20260825-120000-0123456789".parse().unwrap();
    let legacy = CompletionNotice::legacy("ntf_abc", agent.clone(), Status::Succeeded);
    let with_version =
        CompletionNotice::with_version("ntf_abc", agent, Status::Succeeded, NOTICE_VERSION)
            .unwrap();
    assert_eq!(legacy.notification_id, with_version.notification_id);
    assert_eq!(legacy.agent_id, with_version.agent_id);
    assert_eq!(legacy.status, with_version.status);
    assert!(legacy.runtime.is_none());
    assert!(legacy.failure_kind.is_none());
}

/// Mirrors `tests/test_delivery_base.py::test_metadata_must_be_bounded_strings_or_none`.
/// Mirrors `tests/test_delivery_base.py::test_success_and_cancelled_notices_reject_failure_categories`.
/// Mirrors `tests/test_delivery_base.py::test_render_is_the_exact_structured_list`.
/// Mirrors `tests/test_delivery_base.py::test_rendered_message_repeats_only_payload_facts`.
/// Mirrors `tests/test_delivery_base.py::test_notice_carries_trusted_fields_and_a_frozen_legacy_payload`.
#[test]
fn notice_rejects_invalid_metadata_and_contains_only_trusted_facts() {
    let mut invalid = notice();
    invalid.runtime = Some(" ".into());
    assert!(invalid.validate().is_err());
    invalid.runtime = Some("x".repeat(129));
    assert!(invalid.validate().is_err());
    invalid.runtime = None;
    invalid.failure_kind = Some("prepare_failed".into());
    assert!(invalid.validate().is_err());
    let rendered = notice().render().unwrap();
    assert_eq!(rendered.lines().count(), 6);
    assert!(rendered.contains("- Notice: [notification ntf_test v1]"));
    assert!(!rendered.contains("task"));
    assert!(!rendered.contains("answer"));
}

/// Mirrors `tests/test_codex_queue.py::test_validate_and_arguments_are_checked`.
#[test]
fn delivery_configuration_rejects_invalid_retry_arguments() {
    let home = common::Home::new();
    let path = home.path.join("config.toml");
    let original = std::fs::read_to_string(&path).unwrap();
    for delivery in [
        "[delivery]\nretry_base_seconds=0\nretry_cap_seconds=1\nmax_attempts=0\n",
        "[delivery]\nretry_base_seconds=2\nretry_cap_seconds=1\nmax_attempts=0\n",
    ] {
        std::fs::write(&path, format!("{original}\n{delivery}")).unwrap();
        assert!(agent_run_config::config::Config::load(&home.path).is_err());
    }
}

/// Mirrors `tests/test_delivery_dispatch.py::test_cancelled_delivery_stops_retries_and_preserves_the_result`.
#[tokio::test]
async fn cancelled_delivery_is_not_claimed_again() {
    let home = common::Home::new();
    delivery(&home.path, "ntf_cancelled", "codex_queue", "pending");
    let mut store = agent_run_store::fixture::open(&home.path).unwrap();
    assert!(store.cancel_delivery("ntf_cancelled").unwrap());
    drop(store);
    assert_eq!(dispatch_once(&home.path).await.unwrap(), 0);
    let state: String = fixture_connection(home.path.join("state.db"))
        .unwrap()
        .query_row(
            "SELECT state FROM deliveries WHERE id='ntf_cancelled'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(state, "cancelled");
}

/// Mirrors `tests/test_delivery_dispatch.py::test_never_bound_notice_for_a_running_agent_never_expires`.
#[tokio::test]
async fn running_unbound_delivery_never_expires() {
    let home = common::Home::new();
    let connection = fixture_connection(home.path.join("state.db")).unwrap();
    let agent = "ag-20260825-120000-0123456789";
    connection.execute("INSERT INTO agents(id,runtime,model,profile,task,task_summary,workdir,request_json,status,created_at,config_revision,root_agent_id) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)", params![agent,"mock","fixture","review","task","summary",home.path.to_string_lossy(),"{}","running",now()-7200.0,"fixture",agent]).unwrap();
    connection
        .execute(
            "INSERT INTO events(agent_id,at,kind,data_json) VALUES(?,?,?,?)",
            params![agent, now() - 7200.0, "status", "{}"],
        )
        .unwrap();
    let event = connection.last_insert_rowid();
    connection.execute("INSERT INTO deliveries(id,agent_id,terminal_event_seq,state) VALUES(?,?,?,'waiting_binding')", params!["ntf_running",agent,event]).unwrap();
    drop(connection);
    assert_eq!(dispatch_once(&home.path).await.unwrap(), 0);
    let state: String = fixture_connection(home.path.join("state.db"))
        .unwrap()
        .query_row(
            "SELECT state FROM deliveries WHERE id='ntf_running'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(state, "waiting_binding");
}

/// Mirrors `tests/test_delivery_dispatch.py::test_never_bound_notice_inside_the_window_is_left_to_bind`.
#[tokio::test]
async fn recent_unbound_terminal_delivery_waits_for_binding() {
    let home = common::Home::new();
    let connection = fixture_connection(home.path.join("state.db")).unwrap();
    let agent = "ag-20260825-120000-0123456789";
    connection.execute("INSERT INTO agents(id,runtime,model,profile,task,task_summary,workdir,request_json,status,created_at,config_revision,root_agent_id) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)", params![agent,"mock","fixture","review","task","summary",home.path.to_string_lossy(),"{}","succeeded",now()-3599.0,"fixture",agent]).unwrap();
    connection
        .execute(
            "INSERT INTO events(agent_id,at,kind,data_json) VALUES(?,?,?,?)",
            params![agent, now() - 3599.0, "status", "{}"],
        )
        .unwrap();
    let event = connection.last_insert_rowid();
    connection.execute("INSERT INTO deliveries(id,agent_id,terminal_event_seq,state) VALUES(?,?,?,'waiting_binding')", params!["ntf_recent",agent,event]).unwrap();
    drop(connection);
    assert_eq!(dispatch_once(&home.path).await.unwrap(), 0);
    let state: String = fixture_connection(home.path.join("state.db"))
        .unwrap()
        .query_row(
            "SELECT state FROM deliveries WHERE id='ntf_recent'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(state, "waiting_binding");
}

/// Mirrors `tests/test_delivery_dispatch.py::DeliveryDispatchTests::test_unsupported_flock_is_loud_not_contention`
///
/// Advisory locking that cannot be performed at all is a filesystem defect, not
/// a peer holding the dispatcher: it is raised, never silently reported as the
/// quiet "another drain owns it" outcome that would drop the backlog forever.
#[tokio::test]
async fn unusable_dispatcher_lock_is_loud_not_contention() {
    let home = common::Home::new();
    delivery(&home.path, "ntf_flock", "codex_queue", "pending");
    std::fs::create_dir_all(home.path.join("locks/delivery-dispatcher.lock")).unwrap();
    let result = dispatch(&home.path).await;
    let error = result
        .as_ref()
        .err()
        .map(ToString::to_string)
        .unwrap_or_else(|| {
            panic!(
                "an unusable dispatcher lock must be raised, got {:?}",
                result.as_ref().ok()
            )
        });
    assert!(!error.is_empty());
    assert!(
        dispatch_once(&home.path).await.is_err(),
        "the single-shot drain must fail loudly too"
    );
    let state: String = fixture_connection(home.path.join("state.db"))
        .unwrap()
        .query_row(
            "SELECT state FROM deliveries WHERE id='ntf_flock'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        state, "pending",
        "a refused lock must not consume the notice"
    );
}

/// Mirrors `tests/test_delivery_dispatch.py::DeliveryDispatchTests::test_notice_for_tolerates_rows_without_metadata_projection`
///
/// Launch metadata is optional decoration on an already queued notice: a row
/// whose runtime, model, effort, and failure columns are absent or blank still
/// projects into a valid notice and renders the unknown/unspecified placeholders
/// instead of blocking the delivery.
#[tokio::test]
async fn notice_projection_tolerates_rows_without_metadata() {
    let home = common::Home::new();
    delivery_config(&home.path, 1.0, 1.0, 5);
    delivery(&home.path, "ntf_bare", "codex_queue", "pending");
    fixture_connection(home.path.join("state.db"))
        .unwrap()
        .execute(
            "UPDATE agents SET runtime='',model='   ',request_json='',failure_kind=NULL WHERE id=?",
            params!["ag-20260825-120000-0123456789"],
        )
        .unwrap();
    assert_eq!(
        dispatch_once(&home.path).await.unwrap(),
        1,
        "a metadata-free row is still claimed and attempted"
    );
    let row: (String, u32) = fixture_connection(home.path.join("state.db"))
        .unwrap()
        .query_row(
            "SELECT state,attempts FROM deliveries WHERE id='ntf_bare'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(
        row,
        ("retry_wait".into(), 1),
        "absent metadata must not fail the projection"
    );
    let bare = Notice {
        run_id: None,
        notification_id: "ntf_old".into(),
        agent_id: "ag-20260825-120000-0123456789".parse().unwrap(),
        status: Status::Succeeded,
        runtime: None,
        model: None,
        effort: None,
        failure_kind: None,
    };
    assert!(
        bare.render()
            .unwrap()
            .contains("- Runtime/model: unknown/unknown:unspecified\n")
    );
}

/// Mirrors `tests/test_delivery_dispatch.py::DeliveryDispatchTests::test_constructor_validates_config_and_every_transport`
///
/// Python validates the delivery policy once per transport object at dispatcher
/// construction. Rust dispatches on a transport string and has no transport
/// objects, so the equivalent gate is configuration load, which must reject a
/// negative attempt budget and a negative retry ceiling before any drain runs.
#[test]
fn delivery_configuration_rejects_negative_attempt_and_retry_bounds() {
    let home = common::Home::new();
    let path = home.path.join("config.toml");
    let original = std::fs::read_to_string(&path).unwrap();
    for policy in [
        "[delivery]\nmax_attempts=-1\n",
        "[delivery]\nretry_cap_seconds=-1\n",
        "[delivery]\nretry_base_seconds=-1\n",
    ] {
        std::fs::write(&path, format!("{original}\n{policy}")).unwrap();
        assert!(
            agent_run_config::config::Config::load(&home.path).is_err(),
            "invalid delivery policy accepted: {policy}"
        );
    }
    std::fs::write(&path, &original).unwrap();
    agent_run_config::config::Config::load(&home.path)
        .expect("the baseline delivery policy stays loadable");
}

/// A malformed config before admission leaves the row unclaimed and unsent.
#[tokio::test]
async fn malformed_config_before_claim_leaves_delivery_pending() {
    let home = common::Home::new();
    delivery(&home.path, "ntf_cfg_pre", "codex_queue", "pending");
    std::fs::write(home.path.join("config.toml"), "schema_version = [").unwrap();
    assert!(dispatch_once(&home.path).await.is_err());
    let (state, attempts): (String, u32) = fixture_connection(home.path.join("state.db"))
        .unwrap()
        .query_row(
            "SELECT state,attempts FROM deliveries WHERE id='ntf_cfg_pre'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!((state.as_str(), attempts), ("pending", 0));
}

/// Open test SQL with the existing fixture fence; this is setup, never an online production fallback.
fn fixture_connection(path: impl AsRef<Path>) -> rusqlite::Result<Connection> {
    let connection = Connection::open(path)?;
    agent_run_store::fixture::fenced(&connection).expect("fixture fence installs");
    Ok(connection)
}

/// Retain one actual writer/socket per disposable delivery fixture so repeated calls share one connection.
static DELIVERY_WRITERS: std::sync::OnceLock<
    std::sync::Mutex<
        std::collections::HashMap<PathBuf, (common::writer::Harness, common::writer::NativeSocket)>,
    >,
> = std::sync::OnceLock::new();

/// Supply the actual writer only for a nonempty schedule; empty read-only preflight remains observable.
fn ready_writer(home: &Path) -> agent_run_domain::Result<()> {
    if !agent_run_store::writer::StoreReader::open(home)?.delivery_pending(now())? {
        return Ok(());
    }
    let mut writers = DELIVERY_WRITERS
        .get_or_init(Default::default)
        .lock()
        .unwrap();
    writers.entry(home.to_path_buf()).or_insert_with(|| {
        let writer = common::writer::Harness::start(home);
        let socket = writer.serve_unix(home);
        (writer, socket)
    });
    Ok(())
}

/// Execute one production dispatcher call with its real fixture writer when due work exists.
async fn dispatch_once(home: &Path) -> agent_run_domain::Result<usize> {
    ready_writer(home)?;
    real_dispatch_once(home).await
}

/// Execute the bounded production dispatcher with its real fixture writer, preserving original batch limits.
async fn dispatch_with_batch(
    home: &Path,
    batch: usize,
) -> agent_run_domain::Result<agent_run_core::delivery::DispatchResult> {
    ready_writer(home)?;
    real_dispatch_with_batch(home, batch).await
}

/// Execute the default production dispatcher with its actual fixture writer.
async fn dispatch(
    home: &Path,
) -> agent_run_domain::Result<agent_run_core::delivery::DispatchResult> {
    ready_writer(home)?;
    real_dispatch(home).await
}

#[path = "common/bridge.rs"]
mod bridge_fixture;
#[path = "common/bridge_only.rs"]
mod bridge_only;
