//! Broker-client transport parity checks.
//!
//! These tests require a real Unix socket. Each listener is private to a
//! temporary home and is removed with that home after the test completes.

use agent_run::{
    Error,
    transport::{frame, socket},
};
use serde_json::{Value, json};
use std::path::PathBuf;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, BufReader},
    net::UnixListener,
};

/// Builds one private client endpoint for a test server.
fn endpoint(home: &tempfile::TempDir) -> PathBuf {
    home.path().join("api.sock")
}

/// Raw ambiguous batch replies are rejected by both clients without a hidden retry.
#[tokio::test]
async fn batch_clients_reject_duplicate_or_positional_raw_responses() {
    let condition = json!({"agent_id":"ag-20260826-120000-0123456789","sequence":1,
        "execution_ref":format!("ex1_{}","a".repeat(64)),"pin_id":"pin_550e8400e29b41d4a716446655440000"});
    let args = json!({"request_id":"batch-key","expected_store_id":"store_0123456789abcdef0123456789abcdef",
        "preconditions":[condition.clone()],"jobs":[{"provider":"p","model":"m","profile":"review","task":"fixture","workdir":"/missing"}]});
    let receipt = json!({"version":1,"store_id":"store_0123456789abcdef0123456789abcdef","request_id":"batch-key",
        "preconditions":[condition.clone()],"jobs":[{"index":0,"agent_id":"ag-20260826-120001-0123456789","sequence":1,"execution_ref":format!("ex1_{}","b".repeat(64))}]});
    for method in ["start_batch", "batch_receipt"] {
        for case in [
            "duplicate_preconditions",
            "duplicate_null_preconditions",
            "duplicate_selector",
            "duplicate_result",
            "duplicate_null_result",
            "duplicate_id",
            "duplicate_reason",
            "duplicate_null_reason",
            "duplicate_code",
            "timeout_refusal",
            "array_selector",
            "array_job",
            "array_receipt",
            "array_result",
            "result_and_error",
        ] {
            let home = tempfile::tempdir().unwrap();
            let listener = UnixListener::bind(endpoint(&home)).unwrap();
            let condition = condition.clone();
            let receipt = receipt.clone();
            let server = tokio::spawn(async move {
                for _ in 0..2 {
                    let (stream, _) = listener.accept().await.unwrap();
                    let (input, mut output) = stream.into_split();
                    let request: Value = serde_json::from_slice(
                        &frame::read(&mut BufReader::new(input), socket::MAX_FRAME)
                            .await
                            .unwrap()
                            .unwrap(),
                    )
                    .unwrap();
                    assert_eq!(request["method"], method);
                    let id = serde_json::to_string(&request["id"]).unwrap();
                    let mut result = if method == "start_batch" {
                        json!({"replayed":false,"receipt":receipt})
                    } else {
                        json!({"found":true,"receipt":receipt})
                    };
                    match case {
                        "array_selector" => {
                            result["receipt"]["preconditions"][0] = json!([
                                condition["agent_id"],
                                1,
                                condition["execution_ref"],
                                condition["pin_id"]
                            ])
                        }
                        "array_job" => {
                            result["receipt"]["jobs"][0] = json!([
                                0,
                                "ag-20260826-120001-0123456789",
                                1,
                                format!("ex1_{}", "b".repeat(64)),
                                null
                            ])
                        }
                        "array_receipt" => {
                            result["receipt"] = json!([
                                1,
                                receipt["store_id"],
                                "batch-key",
                                receipt["jobs"],
                                receipt["preconditions"]
                            ])
                        }
                        "array_result" if method == "start_batch" => {
                            result = json!([receipt, false])
                        }
                        "array_result" => result = json!([true, receipt]),
                        _ => {}
                    }
                    let raw = match case {
                        "duplicate_reason" => format!(
                            r#"{{"jsonrpc":"2.0","id":{id},"error":{{"code":-32000,"message":"refused","data":{{"code":"PreconditionFailed","reason":"unknown","reason":"not_met"}}}}}}"#
                        ),
                        "duplicate_null_reason" => format!(
                            r#"{{"jsonrpc":"2.0","id":{id},"error":{{"code":-32000,"message":"refused","data":{{"code":"PreconditionFailed","reason":null,"reason":"not_met"}}}}}}"#
                        ),
                        "duplicate_code" => format!(
                            r#"{{"jsonrpc":"2.0","id":{id},"error":{{"code":-32000,"message":"refused","data":{{"code":"RuntimeError","code":"PreconditionFailed","reason":"not_met"}}}}}}"#
                        ),
                        "timeout_refusal" => format!(
                            r#"{{"jsonrpc":"2.0","id":{id},"error":{{"code":-32002,"message":"timeout","data":{{"code":"PreconditionFailed","reason":"not_met"}}}}}}"#
                        ),
                        "duplicate_result" => format!(
                            r#"{{"jsonrpc":"2.0","id":{id},"result":{result},"result":{result}}}"#
                        ),
                        "duplicate_null_result" => format!(
                            r#"{{"jsonrpc":"2.0","id":{id},"result":null,"result":{result}}}"#
                        ),
                        "duplicate_id" => format!(
                            r#"{{"jsonrpc":"2.0","id":"wrong","id":{id},"result":{result}}}"#
                        ),
                        "result_and_error" => format!(
                            r#"{{"jsonrpc":"2.0","id":{id},"result":{result},"error":{{"code":-32000,"message":"refused","data":{{"code":"PreconditionFailed","reason":"not_met"}}}}}}"#
                        ),
                        _ => {
                            let raw = format!(r#"{{"jsonrpc":"2.0","id":{id},"result":{result}}}"#);
                            match case {
                                "duplicate_preconditions" => raw.replacen(
                                    "\"preconditions\":[",
                                    "\"preconditions\":[],\"preconditions\":[",
                                    1,
                                ),
                                "duplicate_null_preconditions" => raw.replacen(
                                    "\"preconditions\":[",
                                    "\"preconditions\":null,\"preconditions\":[",
                                    1,
                                ),
                                "duplicate_selector" => {
                                    let original = condition.to_string();
                                    let duplicate = original
                                        .replace("\"sequence\":1", "\"sequence\":2,\"sequence\":1");
                                    assert_ne!(original, duplicate);
                                    assert!(raw.contains(&original));
                                    raw.replacen(&original, &duplicate, 1)
                                }
                                _ => raw,
                            }
                        }
                    };
                    output.write_all(raw.as_bytes()).await.unwrap();
                    output.write_all(b"\n").await.unwrap();
                }
                assert!(
                    tokio::time::timeout(std::time::Duration::from_millis(200), listener.accept())
                        .await
                        .is_err(),
                    "unexpected hidden retry: {method} {case}"
                );
            });
            for reusable in [false, true] {
                let result = if reusable {
                    socket::BrokerClient::new(endpoint(&home))
                        .call_with_timeout(method, Some(args.clone()), 2.0)
                        .await
                } else {
                    socket::client_at(&endpoint(&home), process_uid(), method, args.clone()).await
                };
                let error = result.expect_err("malformed batch response must fail");
                if method == "start_batch" {
                    assert!(
                        matches!(error, Error::BatchOutcomeUnknown { .. }),
                        "{case}: {error:?}"
                    );
                } else {
                    assert!(matches!(error, Error::Runtime(_)), "{case}: {error:?}");
                }
            }
            server.await.unwrap();
        }
    }
}

/// Returns the current process's effective UID for same-user test servers.
fn process_uid() -> u32 {
    // SAFETY: geteuid is a read-only process identity query.
    unsafe { libc::geteuid() }
}

/// A lost batch acknowledgement never triggers an implicit second submission.
#[tokio::test]
async fn batch_lost_ack_is_unknown_without_hidden_retry() {
    let home = tempfile::tempdir().unwrap();
    let listener = UnixListener::bind(endpoint(&home)).unwrap();
    let args = json!({"request_id":"same-key","jobs":[{"provider":"p","model":"m","profile":"review","task":"example","workdir":"/missing"}]});
    let server = tokio::spawn(async move {
        let mut seen = Vec::new();
        for attempt in 0..2 {
            let (stream, _) = listener.accept().await.unwrap();
            let (input, mut output) = stream.into_split();
            let request: Value = serde_json::from_slice(
                &frame::read(&mut BufReader::new(input), socket::MAX_FRAME)
                    .await
                    .unwrap()
                    .unwrap(),
            )
            .unwrap();
            seen.push(request["params"].clone());
            if attempt == 1 {
                frame::write(
                    &mut output,
                    &json!({"jsonrpc":"2.0","id":request["id"],
                    "error":{"code":-32602,"message":"refused"}}),
                    socket::MAX_FRAME,
                )
                .await
                .unwrap();
            }
        }
        seen
    });
    let client = socket::BrokerClient::new(endpoint(&home));
    let first = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        client.call_with_timeout("start_batch", Some(args.clone()), 2.0),
    )
    .await
    .unwrap()
    .unwrap_err();
    assert!(matches!(first, Error::BatchOutcomeUnknown { .. }));
    let second = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        client.call_with_timeout("start_batch", Some(args.clone()), 2.0),
    )
    .await
    .unwrap()
    .unwrap_err();
    assert!(matches!(second, Error::Validation(_)));
    assert_eq!(server.await.unwrap(), vec![args.clone(), args]);
}

/// A lost hold acknowledgement does not start a second physical mutation.
#[tokio::test]
async fn lineage_hold_mutations_do_not_retry_after_lost_ack() {
    for method in ["_lineage_hold_acquire", "_lineage_hold_release"] {
        let home = tempfile::tempdir().unwrap();
        let listener = UnixListener::bind(endpoint(&home)).unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let (input, output) = stream.into_split();
            let _ = frame::read(&mut BufReader::new(input), socket::MAX_FRAME)
                .await
                .unwrap()
                .unwrap();
            drop(output);
            tokio::time::timeout(std::time::Duration::from_millis(250), listener.accept())
                .await
                .is_err()
        });
        let client = socket::BrokerClient::new(endpoint(&home));
        let error = client
            .call_with_timeout(method, Some(json!({"fixture":true})), 2.0)
            .await
            .unwrap_err();
        assert!(matches!(error, Error::Runtime(_)));
        assert!(server.await.unwrap(), "{method} sent more than once");
    }
}

/// Cancelling the client after a hold request was sent leaves its outcome unknown.
#[tokio::test]
async fn lineage_hold_abort_after_send_reports_unknown() {
    for method in ["_lineage_hold_acquire", "_lineage_hold_release"] {
        let home = tempfile::tempdir().unwrap();
        let listener = UnixListener::bind(endpoint(&home)).unwrap();
        let (seen, received) = tokio::sync::oneshot::channel();
        let (finish, finish_wait) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let (input, _output) = stream.into_split();
            frame::read(&mut BufReader::new(input), socket::MAX_FRAME)
                .await
                .unwrap()
                .unwrap();
            seen.send(()).unwrap();
            let _ = finish_wait.await;
        });
        let client = std::sync::Arc::new(socket::BrokerClient::new(endpoint(&home)));
        let request_client = client.clone();
        let request = tokio::spawn(async move {
            request_client
                .call_with_timeout(method, Some(json!({"fixture":true})), 2.0)
                .await
        });
        received.await.unwrap();
        client.abort();
        let error = request.await.unwrap().unwrap_err();
        assert!(
            matches!(error, Error::Runtime(ref message) if message == "lineage hold outcome unknown")
        );
        finish.send(()).unwrap();
        server.await.unwrap();
    }
}

/// A malformed error after a hold send cannot be interpreted as a refusal.
#[tokio::test]
async fn lineage_hold_malformed_error_is_unknown_without_retry() {
    for method in ["_lineage_hold_acquire", "_lineage_hold_release"] {
        let home = tempfile::tempdir().unwrap();
        let listener = UnixListener::bind(endpoint(&home)).unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let (input, mut output) = stream.into_split();
            let request: Value = serde_json::from_slice(
                &frame::read(&mut BufReader::new(input), socket::MAX_FRAME)
                    .await
                    .unwrap()
                    .unwrap(),
            )
            .unwrap();
            frame::write(
                &mut output,
                &json!({"jsonrpc":"2.0","id":request["id"],"error":{"code":-32602,"message":null}}),
                socket::MAX_FRAME,
            )
            .await
            .unwrap();
            tokio::time::timeout(std::time::Duration::from_millis(250), listener.accept())
                .await
                .is_err()
        });
        let client = socket::BrokerClient::new(endpoint(&home));
        let error = client
            .call_with_timeout(method, Some(json!({"fixture":true})), 2.0)
            .await
            .unwrap_err();
        assert!(
            matches!(error, Error::Runtime(ref message) if message == "lineage hold outcome unknown")
        );
        assert!(server.await.unwrap());
    }
}

/// A later store mismatch does not turn an earlier lost acknowledgement into a refusal.
#[tokio::test]
async fn batch_lost_ack_then_store_mismatch_keeps_original_unknown() {
    let home = tempfile::tempdir().unwrap();
    let listener = UnixListener::bind(endpoint(&home)).unwrap();
    let args = json!({"request_id":"same-key","expected_store_id":"store_0123456789abcdef0123456789abcdef","jobs":[{"provider":"p","model":"m","profile":"review","task":"example","workdir":"/missing"}]});
    let server = tokio::spawn(async move {
        let mut seen = Vec::new();
        for attempt in 0..2 {
            let (stream, _) = listener.accept().await.unwrap();
            let (input, mut output) = stream.into_split();
            let request: Value = serde_json::from_slice(
                &frame::read(&mut BufReader::new(input), socket::MAX_FRAME)
                    .await
                    .unwrap()
                    .unwrap(),
            )
            .unwrap();
            seen.push(request["params"].clone());
            if attempt == 1 {
                frame::write(&mut output, &json!({"jsonrpc":"2.0","id":request["id"],"error":{
                    "code":-32000,"message":"store identity mismatch","data":{"code":"StoreIdentityMismatch"}}}), socket::MAX_FRAME).await.unwrap();
            }
        }
        seen
    });
    let client = socket::BrokerClient::new(endpoint(&home));
    let first = client
        .call_with_timeout("start_batch", Some(args.clone()), 2.0)
        .await
        .unwrap_err();
    assert!(matches!(first, Error::BatchOutcomeUnknown { .. }));
    let second = client
        .call_with_timeout("start_batch", Some(args.clone()), 2.0)
        .await
        .unwrap_err();
    assert_eq!(second.machine_code().as_str(), "StoreIdentityMismatch");
    assert_eq!(server.await.unwrap(), vec![args.clone(), args]);
}

/// Both client types preserve only valid machine-readable refusal reasons.
#[tokio::test]
async fn batch_precondition_error_reasons_are_checked_by_both_clients() {
    let home = tempfile::tempdir().unwrap();
    let listener = UnixListener::bind(endpoint(&home)).unwrap();
    let args = json!({"request_id":"same-key","expected_store_id":"store_0123456789abcdef0123456789abcdef",
        "preconditions":[{"agent_id":"ag-20260826-120000-0123456789","sequence":1,"execution_ref":format!("ex1_{}","a".repeat(64)),"pin_id":"pin_550e8400e29b41d4a716446655440000"}],
        "jobs":[{"provider":"p","model":"m","profile":"review","task":"example","workdir":"/missing"}]});
    let server = tokio::spawn(async move {
        for reason in [
            Some("not_met"),
            Some("not_met"),
            Some("unknown"),
            Some("unknown"),
            None,
            None,
            Some("bogus"),
            Some("bogus"),
        ] {
            let (stream, _) = listener.accept().await.unwrap();
            let (input, mut output) = stream.into_split();
            let request: Value = serde_json::from_slice(
                &frame::read(&mut BufReader::new(input), socket::MAX_FRAME)
                    .await
                    .unwrap()
                    .unwrap(),
            )
            .unwrap();
            let mut data = json!({"code":"PreconditionFailed"});
            if let Some(reason) = reason {
                data["reason"] = json!(reason);
            }
            frame::write(&mut output, &json!({"jsonrpc":"2.0","id":request["id"],"error":{"code":-32000,"message":"condition failed","data":data}}), socket::MAX_FRAME).await.unwrap();
        }
    });
    for expected in [Some("not_met"), Some("unknown"), None, None] {
        let one_shot =
            socket::client_at(&endpoint(&home), process_uid(), "start_batch", args.clone())
                .await
                .unwrap_err();
        let reusable = socket::BrokerClient::new(endpoint(&home));
        let other = reusable
            .call("start_batch", Some(args.clone()))
            .await
            .unwrap_err();
        for error in [one_shot, other] {
            match expected {
                Some(reason) => {
                    assert_eq!(error.machine_code().as_str(), "PreconditionFailed");
                    assert_eq!(
                        serde_json::to_value(error.public().reason).unwrap(),
                        json!(reason)
                    );
                }
                None => assert!(matches!(error, Error::BatchOutcomeUnknown { .. })),
            }
        }
    }
    server.await.unwrap();
}

/// A later failed precondition describes only that submit, never the outcome
/// of an earlier lost ACK; a missing read-only receipt is also inconclusive.
#[tokio::test]
async fn batch_lost_ack_then_precondition_refusal_remains_unresolved() {
    let home = tempfile::tempdir().unwrap();
    let listener = UnixListener::bind(endpoint(&home)).unwrap();
    let args = json!({"request_id":"same-key","expected_store_id":"store_0123456789abcdef0123456789abcdef",
        "preconditions":[{"agent_id":"ag-20260826-120000-0123456789","sequence":1,"execution_ref":format!("ex1_{}","a".repeat(64)),"pin_id":"pin_550e8400e29b41d4a716446655440000"}],
        "jobs":[{"provider":"p","model":"m","profile":"review","task":"example","workdir":"/missing"}]});
    let server = tokio::spawn(async move {
        let mut seen = Vec::new();
        for index in 0..3 {
            let (stream, _) = listener.accept().await.unwrap();
            let (input, mut output) = stream.into_split();
            let request: Value = serde_json::from_slice(
                &frame::read(&mut BufReader::new(input), socket::MAX_FRAME)
                    .await
                    .unwrap()
                    .unwrap(),
            )
            .unwrap();
            seen.push(request["method"].clone());
            if index == 1 {
                frame::write(&mut output, &json!({"jsonrpc":"2.0","id":request["id"],"error":{"code":-32000,"message":"condition failed","data":{"code":"PreconditionFailed","reason":"not_met"}}}), socket::MAX_FRAME).await.unwrap();
            } else if index == 2 {
                frame::write(
                    &mut output,
                    &json!({"jsonrpc":"2.0","id":request["id"],"result":{"found":false}}),
                    socket::MAX_FRAME,
                )
                .await
                .unwrap();
            }
        }
        seen
    });
    let client = socket::BrokerClient::new(endpoint(&home));
    let original = client
        .call_with_timeout("start_batch", Some(args.clone()), 2.0)
        .await
        .unwrap_err();
    assert!(matches!(&original, Error::BatchOutcomeUnknown { .. }));
    let current = client
        .call_with_timeout("start_batch", Some(args.clone()), 2.0)
        .await
        .unwrap_err();
    assert_eq!(current.machine_code().as_str(), "PreconditionFailed");
    assert_eq!(
        serde_json::to_value(current.public().reason).unwrap(),
        "not_met"
    );
    assert!(matches!(&original, Error::BatchOutcomeUnknown { .. }));
    let lookup = socket::client_at(&endpoint(&home), process_uid(), "batch_receipt", args)
        .await
        .unwrap();
    assert_eq!(lookup, json!({"found":false}));
    assert!(matches!(&original, Error::BatchOutcomeUnknown { .. }));
    assert_eq!(
        server.await.unwrap(),
        vec![
            json!("start_batch"),
            json!("start_batch"),
            json!("batch_receipt")
        ]
    );
}

/// Both socket clients reject malformed opt-in store identities.
#[tokio::test]
async fn capabilities_store_id_requires_canonical_reply() {
    let home = tempfile::tempdir().unwrap();
    let listener = UnixListener::bind(endpoint(&home)).unwrap();
    let server = tokio::spawn(async move {
        for attempt in 0..4 {
            let (stream, _) = listener.accept().await.unwrap();
            let (input, mut output) = stream.into_split();
            let request: Value = serde_json::from_slice(
                &frame::read(&mut BufReader::new(input), socket::MAX_FRAME)
                    .await
                    .unwrap()
                    .unwrap(),
            )
            .unwrap();
            let result = match attempt {
                0 => json!({}),
                1 => json!({"store_id":"store_BROKEN"}),
                _ => json!({"store_id":"store_0123456789abcdef0123456789abcdef"}),
            };
            frame::write(
                &mut output,
                &json!({"jsonrpc":"2.0","id":request["id"],"result":result}),
                socket::MAX_FRAME,
            )
            .await
            .unwrap();
        }
    });
    let args = json!({"include_store_id":true});
    let reusable = socket::BrokerClient::new(endpoint(&home));
    assert!(
        socket::client_at(
            &endpoint(&home),
            process_uid(),
            "capabilities",
            args.clone()
        )
        .await
        .is_err()
    );
    assert!(
        reusable
            .call("capabilities", Some(args.clone()))
            .await
            .is_err()
    );
    assert!(
        socket::client_at(
            &endpoint(&home),
            process_uid(),
            "capabilities",
            args.clone()
        )
        .await
        .is_ok()
    );
    assert!(reusable.call("capabilities", Some(args)).await.is_ok());
    server.await.unwrap();
}

/// Both socket clients reject a status result for a different selector.
#[tokio::test]
async fn status_socket_clients_validate_selector_correlation() {
    let home = tempfile::tempdir().unwrap();
    let listener = UnixListener::bind(endpoint(&home)).unwrap();
    let id = "ag-20260826-120000-0123456789";
    let args = json!({"version":1,"targets":[{"agent_id":id,"sequence":1}]});
    let server = tokio::spawn(async move {
        for attempt in 0..4 {
            let (stream, _) = listener.accept().await.unwrap();
            let (input, mut output) = stream.into_split();
            let request: Value = serde_json::from_slice(
                &frame::read(&mut BufReader::new(input), socket::MAX_FRAME)
                    .await
                    .unwrap()
                    .unwrap(),
            )
            .unwrap();
            let sequence = if attempt < 2 { 2 } else { 1 };
            let result = json!({"version":1,"store_id":"store_0123456789abcdef0123456789abcdef","observed_at":1.0,"items":[{
                "selector":{"agent_id":id,"sequence":sequence},"canonical_agent_id":null,"match":"missing","pin_id":null,"pin_state":"missing",
                "selected":null,"retained_latest":null,"coverage":"unknown","restart_resume":"unknown","lineage_terminal":null,
                "terminal_outcome":null,"success_commit":"unknown","cleanup":{"scope":"lineage","state":"unknown"}}]});
            frame::write(
                &mut output,
                &json!({"jsonrpc":"2.0","id":request["id"],"result":result}),
                socket::MAX_FRAME,
            )
            .await
            .unwrap();
        }
    });
    let reusable = socket::BrokerClient::new(endpoint(&home));
    assert!(
        socket::client_at(&endpoint(&home), process_uid(), "status", args.clone())
            .await
            .is_err()
    );
    assert!(reusable.call("status", Some(args.clone())).await.is_err());
    let one_shot = socket::client_at(&endpoint(&home), process_uid(), "status", args.clone())
        .await
        .unwrap();
    let again = reusable.call("status", Some(args)).await.unwrap();
    assert_eq!(one_shot, again);
    server.await.unwrap();
}

/// Both socket clients validate the found/missing lookup and its store scope.
#[tokio::test]
async fn batch_receipt_socket_clients_validate_found_and_scope() {
    let home = tempfile::tempdir().unwrap();
    let listener = UnixListener::bind(endpoint(&home)).unwrap();
    let args = json!({"request_id":"lookup","expected_store_id":"store_0123456789abcdef0123456789abcdef",
        "jobs":[{"provider":"p","model":"m","profile":"review","task":"fixture","workdir":"/missing"}]});
    let server = tokio::spawn(async move {
        for attempt in 0..4 {
            let (stream, _) = listener.accept().await.unwrap();
            let (input, mut output) = stream.into_split();
            let request: Value = serde_json::from_slice(
                &frame::read(&mut BufReader::new(input), socket::MAX_FRAME)
                    .await
                    .unwrap()
                    .unwrap(),
            )
            .unwrap();
            let mut found = json!({"found":true,"receipt":{"version":1,"store_id":"store_0123456789abcdef0123456789abcdef","request_id":"lookup","jobs":[{
                "index":0,"agent_id":"ag-20260826-120000-0123456789","sequence":1,"execution_ref":format!("ex1_{}","a".repeat(64))}]}});
            let result = match attempt {
                0 => json!({"found":false,"receipt":null}),
                1 => {
                    found["receipt"]["store_id"] = json!("store_ffffffffffffffffffffffffffffffff");
                    found
                }
                2 => json!({"found":false}),
                _ => found,
            };
            frame::write(
                &mut output,
                &json!({"jsonrpc":"2.0","id":request["id"],"result":result}),
                socket::MAX_FRAME,
            )
            .await
            .unwrap();
        }
    });
    let reusable = socket::BrokerClient::new(endpoint(&home));
    assert!(
        socket::client_at(
            &endpoint(&home),
            process_uid(),
            "batch_receipt",
            args.clone()
        )
        .await
        .is_err()
    );
    assert!(
        reusable
            .call("batch_receipt", Some(args.clone()))
            .await
            .is_err()
    );
    assert_eq!(
        socket::client_at(
            &endpoint(&home),
            process_uid(),
            "batch_receipt",
            args.clone()
        )
        .await
        .unwrap(),
        json!({"found":false})
    );
    assert_eq!(
        reusable.call("batch_receipt", Some(args)).await.unwrap()["found"],
        true
    );
    server.await.unwrap();
}

/// A lost admission ACK is reconciled by lookup without a second start request.
#[tokio::test]
async fn lost_batch_ack_then_read_only_receipt_lookup() {
    let home = tempfile::tempdir().unwrap();
    let listener = UnixListener::bind(endpoint(&home)).unwrap();
    let args = json!({"request_id":"lookup","expected_store_id":"store_0123456789abcdef0123456789abcdef",
        "jobs":[{"provider":"p","model":"m","profile":"review","task":"fixture","workdir":"/missing"}]});
    let server = tokio::spawn(async move {
        let mut methods = Vec::new();
        for index in 0..2 {
            let (stream, _) = listener.accept().await.unwrap();
            let (input, mut output) = stream.into_split();
            let request: Value = serde_json::from_slice(
                &frame::read(&mut BufReader::new(input), socket::MAX_FRAME)
                    .await
                    .unwrap()
                    .unwrap(),
            )
            .unwrap();
            methods.push(request["method"].as_str().unwrap().to_owned());
            if index == 1 {
                let result = json!({"found":true,"receipt":{"version":1,"store_id":"store_0123456789abcdef0123456789abcdef","request_id":"lookup","jobs":[{
                    "index":0,"agent_id":"ag-20260826-120000-0123456789","sequence":1,"execution_ref":format!("ex1_{}","a".repeat(64))}]}});
                frame::write(
                    &mut output,
                    &json!({"jsonrpc":"2.0","id":request["id"],"result":result}),
                    socket::MAX_FRAME,
                )
                .await
                .unwrap();
            }
        }
        methods
    });
    let client = socket::BrokerClient::new(endpoint(&home));
    assert!(matches!(
        client
            .call_with_timeout("start_batch", Some(args.clone()), 2.0)
            .await,
        Err(Error::BatchOutcomeUnknown { .. })
    ));
    assert_eq!(
        client
            .call_with_timeout("batch_receipt", Some(args), 2.0)
            .await
            .unwrap()["found"],
        true
    );
    assert_eq!(server.await.unwrap(), vec!["start_batch", "batch_receipt"]);
}

/// Correlation and receipt failures after send are unknown, unlike a correlated refusal.
#[tokio::test]
async fn batch_one_shot_checks_response_without_changing_scalar_errors() {
    let home = tempfile::tempdir().unwrap();
    let listener = UnixListener::bind(endpoint(&home)).unwrap();
    let args = json!({"request_id":"same-key","jobs":[{"provider":"p","model":"m","profile":"review","task":"example","workdir":"/missing"}]});
    let server = tokio::spawn(async move {
        for attempt in 0..5 {
            let (stream, _) = listener.accept().await.unwrap();
            let (input, mut output) = stream.into_split();
            let request: Value = serde_json::from_slice(
                &frame::read(&mut BufReader::new(input), socket::MAX_FRAME)
                    .await
                    .unwrap()
                    .unwrap(),
            )
            .unwrap();
            assert_eq!(request["params"]["request_id"], "same-key");
            let response = match attempt {
                0 | 4 => json!({"jsonrpc":"2.0","id":"wrong","result":{}}),
                1 => json!({"jsonrpc":"2.0","id":request["id"]}),
                2 => json!({"jsonrpc":"2.0","id":request["id"],"result":{}}),
                _ => {
                    json!({"jsonrpc":"2.0","id":request["id"],"error":{"code":-32602,"message":"refused"}})
                }
            };
            frame::write(&mut output, &response, socket::MAX_FRAME)
                .await
                .unwrap();
        }
    });
    for attempt in 0..5 {
        let method = if attempt == 4 { "start" } else { "start_batch" };
        let error = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            socket::client_at(&endpoint(&home), process_uid(), method, args.clone()),
        )
        .await
        .unwrap()
        .unwrap_err();
        if attempt < 3 {
            assert!(matches!(error, Error::BatchOutcomeUnknown { .. }));
        } else {
            assert!(matches!(error, Error::Validation(_)));
        }
    }
    server.await.unwrap();
}

/// Both socket clients reject missing or malformed execution incarnations.
#[tokio::test]
async fn batch_socket_clients_validate_execution_ref() {
    let home = tempfile::tempdir().unwrap();
    let listener = UnixListener::bind(endpoint(&home)).unwrap();
    let args = json!({"request_id":"same-key","jobs":[{"provider":"p","model":"m","profile":"review","task":"example","workdir":"/missing"}]});
    let server = tokio::spawn(async move {
        for attempt in 0..4 {
            let (stream, _) = listener.accept().await.unwrap();
            let (input, mut output) = stream.into_split();
            let request: Value = serde_json::from_slice(
                &frame::read(&mut BufReader::new(input), socket::MAX_FRAME)
                    .await
                    .unwrap()
                    .unwrap(),
            )
            .unwrap();
            let mut job =
                json!({"index":0,"agent_id":"ag-20260826-120000-0123456789","sequence":1});
            if attempt == 1 {
                job["execution_ref"] = json!("ex1_invalid");
            }
            if attempt >= 2 {
                job["execution_ref"] = json!(format!("ex1_{}", "a".repeat(64)));
            }
            let result = json!({"replayed":false,"receipt":{"version":1,"store_id":"store_0123456789abcdef0123456789abcdef","request_id":"same-key","jobs":[job]}});
            frame::write(
                &mut output,
                &json!({"jsonrpc":"2.0","id":request["id"],"result":result}),
                socket::MAX_FRAME,
            )
            .await
            .unwrap();
        }
    });
    let reusable = socket::BrokerClient::new(endpoint(&home));
    assert!(matches!(
        socket::client_at(&endpoint(&home), process_uid(), "start_batch", args.clone()).await,
        Err(Error::BatchOutcomeUnknown { .. })
    ));
    assert!(matches!(
        reusable.call("start_batch", Some(args.clone())).await,
        Err(Error::BatchOutcomeUnknown { .. })
    ));
    let one_shot = socket::client_at(&endpoint(&home), process_uid(), "start_batch", args.clone())
        .await
        .unwrap();
    let again = reusable.call("start_batch", Some(args)).await.unwrap();
    assert_eq!(one_shot, again);
    assert_eq!(
        one_shot["receipt"]["jobs"][0]["execution_ref"],
        format!("ex1_{}", "a".repeat(64))
    );
    server.await.unwrap();
}

/// A lost resume response retries the same intent rather than continuing again.
#[tokio::test]
async fn resume_transport_retry_keeps_one_generated_request_id() {
    let home = tempfile::tempdir().unwrap();
    let listener = UnixListener::bind(endpoint(&home)).unwrap();
    let server_uid = process_uid();
    let server = tokio::spawn(async move {
        let mut keys = Vec::new();
        for attempt in 0..2 {
            let (stream, _) = listener.accept().await.unwrap();
            let (input, mut output) = stream.into_split();
            let mut input = BufReader::new(input);
            let request: Value = serde_json::from_slice(
                &frame::read(&mut input, socket::MAX_FRAME)
                    .await
                    .unwrap()
                    .unwrap(),
            )
            .unwrap();
            keys.push(request["params"]["request_id"].as_str().unwrap().to_owned());
            if attempt == 1 {
                frame::write(
                    &mut output,
                    &json!({
                        "jsonrpc":"2.0", "id":request["id"],
                        "result":{"agent_id":"root","run_id":"child","created":false}
                    }),
                    socket::MAX_FRAME,
                )
                .await
                .unwrap();
            }
        }
        keys
    });
    let result = socket::BrokerClient::with_expected_server_uid(endpoint(&home), server_uid)
        .call_with_timeout(
            "resume",
            Some(json!({"agent_id":"root","task":"continue"})),
            5.0,
        )
        .await
        .unwrap();
    assert_eq!(result["run_id"], "child");
    let keys = tokio::time::timeout(std::time::Duration::from_secs(5), server)
        .await
        .unwrap()
        .unwrap();
    assert!(!keys[0].is_empty());
    assert_eq!(keys[0], keys[1]);
}

/// A mismatched trusted UID is rejected before any request bytes on each connection.
#[tokio::test]
async fn configured_server_uid_rejects_mismatch_before_request() {
    let home = tempfile::tempdir().unwrap();
    let path = endpoint(&home);
    let listener = UnixListener::bind(&path).unwrap();
    let server_uid = process_uid();
    let server = tokio::spawn(async move {
        for _ in 0..2 {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut byte = [0; 1];
            assert_eq!(stream.read(&mut byte).await.unwrap(), 0);
        }
    });
    let client = socket::BrokerClient::with_expected_server_uid(path, server_uid.wrapping_add(1));
    for _ in 0..2 {
        let error = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            client.call("capacity_order", None),
        )
        .await
        .expect("UID refusal deadline")
        .unwrap_err();
        assert!(
            matches!(error, Error::Validation(message) if message == "broker belongs to another user")
        );
    }
    tokio::time::timeout(std::time::Duration::from_secs(5), server)
        .await
        .unwrap()
        .unwrap();
}

/// The one-shot client checks its explicit server UID before writing a request.
#[tokio::test]
async fn client_at_checks_server_uid_before_request() {
    let home = tempfile::tempdir().unwrap();
    let path = endpoint(&home);
    let listener = UnixListener::bind(&path).unwrap();
    let server = tokio::spawn(async move {
        let (mut rejected, _) = listener.accept().await.unwrap();
        let mut byte = [0; 1];
        assert_eq!(rejected.read(&mut byte).await.unwrap(), 0);

        let (stream, _) = listener.accept().await.unwrap();
        let (input, mut output) = stream.into_split();
        let request: Value = serde_json::from_slice(
            &frame::read(&mut BufReader::new(input), socket::MAX_FRAME)
                .await
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        frame::write(
            &mut output,
            &json!({"jsonrpc":"2.0", "id":request["id"], "result":{"ok":true}}),
            socket::MAX_FRAME,
        )
        .await
        .unwrap();
    });
    let error = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        socket::client_at(
            &path,
            process_uid().wrapping_add(1),
            "capacity_order",
            json!({}),
        ),
    )
    .await
    .expect("UID refusal deadline")
    .unwrap_err();
    assert!(
        matches!(error, Error::Validation(message) if message == "broker belongs to another user")
    );
    assert_eq!(
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            socket::client_at(&path, process_uid(), "capacity_order", json!({})),
        )
        .await
        .expect("UID-checked call deadline")
        .unwrap(),
        json!({"ok":true})
    );
    tokio::time::timeout(std::time::Duration::from_secs(5), server)
        .await
        .unwrap()
        .unwrap();
}

/// Mirrors `test_broker_client.py::test_validation_error_mapping`.
#[tokio::test]
async fn client_maps_a_validation_envelope_to_a_typed_error() {
    let home = tempfile::tempdir().expect("temporary home");
    let listener = UnixListener::bind(home.path().join("api.sock")).expect("private listener");
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("client connects");
        let (input, mut output) = stream.into_split();
        let mut input = BufReader::new(input);
        let request: Value = serde_json::from_slice(
            &frame::read(&mut input, socket::MAX_FRAME)
                .await
                .expect("read request")
                .expect("request frame"),
        )
        .expect("JSON request");
        frame::write(
            &mut output,
            &json!({
                "jsonrpc": "2.0", "id": request["id"],
                "error": {"code": -32602, "message": "bad params"}
            }),
            socket::MAX_FRAME,
        )
        .await
        .expect("write response");
    });

    let error = socket::client(home.path(), "capacity_order", json!({}))
        .await
        .expect_err("validation response is an error");
    assert!(matches!(error, Error::Validation(message) if message == "bad params"));
    server.await.expect("server exits");
}

/// Mirrors `tests/test_broker_client.py::test_round_trip_and_monotonic_ids`.
#[tokio::test]
async fn client_reuses_a_connection_with_monotonic_ids() {
    let home = tempfile::tempdir().unwrap();
    let listener = UnixListener::bind(endpoint(&home)).unwrap();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let (input, mut output) = stream.into_split();
        let mut input = BufReader::new(input);
        let mut ids = Vec::new();
        for _ in 0..2 {
            let request: Value = serde_json::from_slice(
                &frame::read(&mut input, socket::MAX_FRAME)
                    .await
                    .unwrap()
                    .unwrap(),
            )
            .unwrap();
            ids.push(request["id"].as_u64().unwrap());
            frame::write(
                &mut output,
                &json!({"jsonrpc":"2.0","id":request["id"],"result":{"value":request["params"]}}),
                socket::MAX_FRAME,
            )
            .await
            .unwrap();
        }
        ids
    });
    let client = socket::BrokerClient::new(endpoint(&home));
    assert_eq!(
        client
            .call("capacity_order", Some(json!({"x":1})))
            .await
            .unwrap(),
        json!({"value":{"x":1}})
    );
    assert_eq!(
        client.call("answer", Some(json!({"x":2}))).await.unwrap(),
        json!({"value":{"x":2}})
    );
    assert_eq!(server.await.unwrap(), vec![1, 2]);
}

/// Mirrors `tests/test_broker_client.py::test_start_serializes_request_and_rejects_malformed_results`.
#[tokio::test]
async fn client_start_serializes_request_and_rejects_malformed_results() {
    let home = tempfile::tempdir().unwrap();
    let listener = UnixListener::bind(endpoint(&home)).unwrap();
    let workdir = home.path().to_path_buf();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let (input, mut output) = stream.into_split();
        let mut input = BufReader::new(input);
        let mut seen = Vec::new();
        for index in 0..2 {
            let request: Value = serde_json::from_slice(
                &frame::read(&mut input, socket::MAX_FRAME)
                    .await
                    .unwrap()
                    .unwrap(),
            )
            .unwrap();
            seen.push(request.clone());
            let result = if index == 0 {
                json!({"agent_id":"ag-test","attempt_id":"at-test","created":true})
            } else {
                json!({})
            };
            frame::write(
                &mut output,
                &json!({"jsonrpc":"2.0","id":request["id"],"result":result}),
                socket::MAX_FRAME,
            )
            .await
            .unwrap();
        }
        seen
    });
    let client = socket::BrokerClient::new(endpoint(&home));
    let request: agent_run_domain::ProviderStartRequest = serde_json::from_value(json!({
        "provider": "codex-user", "model": "model", "profile": "review",
        "task": "task", "workdir": workdir,
    }))
    .unwrap();
    let result = client.start(&request).await.unwrap();
    assert_eq!(
        (
            result.agent_id.as_str(),
            result.created,
            result.attempt_id.as_deref()
        ),
        ("ag-test", true, Some("at-test"))
    );
    let error = client.start(&request).await.unwrap_err();
    assert!(
        matches!(error, Error::Runtime(message) if message == "broker returned an invalid start result")
    );
    let seen = server.await.unwrap();
    assert_eq!(seen[0]["method"], "start");
    // The strict provider shape the dispatcher accepts, never `runtime`.
    assert_eq!(seen[0]["params"]["provider"], "codex-user");
    assert!(seen[0]["params"].get("runtime").is_none());
    serde_json::from_value::<agent_run_domain::ProviderStartRequest>(seen[0]["params"].clone())
        .unwrap();
    assert_eq!(
        seen[0]["params"]["workdir"],
        home.path().to_string_lossy().as_ref()
    );
}

/// Mirrors `tests/test_broker_client.py::test_reconnects_once_after_server_restart`.
#[tokio::test]
async fn client_reconnects_once_after_server_restart() {
    let home = tempfile::tempdir().unwrap();
    let path = endpoint(&home);
    let first = UnixListener::bind(&path).unwrap();
    let first_server = tokio::spawn(async move {
        let (stream, _) = first.accept().await.unwrap();
        drop(stream);
    });
    let client = socket::BrokerClient::new(&path);
    let _ = client.call("ping", None).await;
    first_server.await.unwrap();
    std::fs::remove_file(&path).unwrap();
    let second = UnixListener::bind(&path).unwrap();
    let second_server = tokio::spawn(async move {
        let (stream, _) = second.accept().await.unwrap();
        let (input, mut output) = stream.into_split();
        let mut input = BufReader::new(input);
        let request: Value = serde_json::from_slice(
            &frame::read(&mut input, socket::MAX_FRAME)
                .await
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        frame::write(
            &mut output,
            &json!({"jsonrpc":"2.0","id":request["id"],"result":{"restarted":true}}),
            socket::MAX_FRAME,
        )
        .await
        .unwrap();
    });
    assert_eq!(
        client.call("capacity_order", None).await.unwrap(),
        json!({"restarted":true})
    );
    second_server.await.unwrap();
}

/// Mirrors `tests/test_broker_client.py::test_unavailable_has_actionable_message`.
#[tokio::test]
async fn client_unavailable_error_is_actionable() {
    let home = tempfile::tempdir().unwrap();
    let error = socket::BrokerClient::new(endpoint(&home))
        .call("capacity_order", None)
        .await
        .unwrap_err();
    assert!(matches!(error, Error::BrokerUnavailable));
    assert!(error.to_string().contains("agent-run api serve"));
}

/// Mirrors `tests/test_broker_client.py::test_invalid_deadlines_are_rejected_before_socket_creation`.
#[tokio::test]
async fn client_rejects_invalid_deadlines_before_connecting() {
    let home = tempfile::tempdir().unwrap();
    for timeout in [0.0, -1.0, f64::INFINITY, f64::NAN] {
        let error = socket::BrokerClient::new(endpoint(&home))
            .call_with_timeout("capacity_order", None, timeout)
            .await
            .unwrap_err();
        assert!(
            matches!(error, Error::Validation(message) if message.contains("positive and finite"))
        );
    }
}

/// Mirrors `tests/test_broker_client.py::test_agent_error_mapping_preserves_data_code`.
#[tokio::test]
async fn client_preserves_agent_error_mapping_data_code() {
    let home = tempfile::tempdir().unwrap();
    let listener = UnixListener::bind(endpoint(&home)).unwrap();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let (input, mut output) = stream.into_split();
        let mut input = BufReader::new(input);
        let request: Value = serde_json::from_slice(
            &frame::read(&mut input, socket::MAX_FRAME)
                .await
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        frame::write(&mut output, &json!({"jsonrpc":"2.0","id":request["id"],"error":{"code":-32000,"message":"domain failure","data":{"code":"AuthError","message":"domain failure"}}}), socket::MAX_FRAME).await.unwrap();
    });
    let error = socket::BrokerClient::new(endpoint(&home))
        .call("capacity_order", None)
        .await
        .unwrap_err();
    assert!(
        matches!(error, Error::Broker { broker_error_code: Some(ref code), .. } if code == "AuthError")
    );
    assert_eq!(error.to_string(), "domain failure");
    server.await.unwrap();
}

/// Mirrors `tests/test_broker_client.py::test_abort_interrupts_each_connect_without_retry_or_worker_leak`.
#[tokio::test]
async fn client_abort_interrupts_a_pending_connect_without_retry() {
    let home = tempfile::tempdir().unwrap();
    let client = socket::BrokerClient::new(endpoint(&home));
    client.abort();
    let error = client.call("capacity_order", None).await.unwrap_err();
    assert!(matches!(error, Error::Runtime(message) if message == "broker call cancelled"));
}
