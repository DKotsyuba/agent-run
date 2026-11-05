//! Deterministic Claude maintenance evidence; no processes, model or network.
use agent_run_core::stream::maintenance::{Assessment, Gap, Observer};
use agent_run_domain::worker::MaintenancePauseRequest;
use serde_json::{Value, json};

const ATTEMPT: &str = "attempt-fixture";
const SESSION: &str = "native-session";
const INPUT: &str = "11111111-1111-4111-8111-111111111111";
const OTHER_INPUT: &str = "22222222-2222-4222-8222-222222222222";

/// Exact synthetic request and the digest independently supplied by the store.
fn request() -> (MaintenancePauseRequest, String) {
    let input = MaintenancePauseRequest {
        challenge_id: "a".repeat(64),
        checkpoint: "private checkpoint".into(),
    };
    let digest =
        agent_run_domain::canonical::sha256_hex(&serde_json::to_value(&input).unwrap(), true);
    (input, digest)
}

/// Fresh owned attempt with one runner-sent input and an owned native session.
fn observer() -> Observer {
    let mut observer = Observer::new(ATTEMPT, false);
    observer.input_sent(INPUT);
    observer.observe(
        &json!({"type":"system","subtype":"init","session_id":SESSION}),
        None,
    );
    let (input, digest) = request();
    observer.expect_checkpoint(ATTEMPT, SESSION, INPUT, &input.challenge_id, &digest);
    observer
}

/// A fresh turn before the maintenance checkpoint has been requested.
fn foreground_observer() -> Observer {
    let mut observer = Observer::new(ATTEMPT, false);
    observer.input_sent(INPUT);
    observer.observe(
        &json!({"type":"system","subtype":"init","session_id":SESSION}),
        None,
    );
    observer
}

/// Complete private tool invocation under one assistant message.
fn started() -> Value {
    json!({"type":"assistant","session_id":SESSION,"parent_tool_use_id":null,"message":{"id":"message-1","role":"assistant","content":[{
        "type":"tool_use","id":"pause-1","name":"mcp__agent_run_worker__maintenance_pause","input":request().0
    }]}})
}

/// Tool result in the same session, with one content-free typed receipt.
fn receipt() -> Value {
    json!({"type":"user","session_id":SESSION,"parent_tool_use_id":null,"message":{"role":"user","content":[{
        "type":"tool_result","tool_use_id":"pause-1","is_error":false,
        "content":json!({"sha256":request().1,"duplicate":false}).to_string()
    }]}})
}

/// Native result must be successful and name the exact runner-sent UUID.
fn result() -> Value {
    json!({"type":"result","session_id":SESSION,"user_message_uuid":INPUT,"subtype":"success","is_error":false})
}

/// A broker-bound exact receipt and its later matching turn result are both
/// required; settling makes no process or external-effect claim.
#[test]
fn claude_maintenance_exact_receipt_then_correlated_result_settles() {
    let mut first = observer();
    assert_eq!(first.assessment(), Assessment::AwaitingReceipt);
    first.observe(&started(), None);
    first.observe(&receipt(), None);
    assert_eq!(first.assessment(), Assessment::AwaitingTurnCompletion);
    first.observe(&result(), Some(true));
    assert_eq!(first.assessment(), Assessment::Settled);
    let mut array_receipt = receipt();
    array_receipt["message"]["content"][0]["content"] = json!([
        {"type":"text","text":json!({"sha256":request().1,"duplicate":true}).to_string()}
    ]);
    let mut retry = observer();
    retry.observe(&started(), None);
    retry.observe(&array_receipt, None);
    retry.observe(&result(), Some(true));
    assert_eq!(retry.assessment(), Assessment::Settled);
    let debug = format!("{first:?}");
    let (input, digest) = request();
    for private in [
        ATTEMPT,
        SESSION,
        INPUT,
        "private checkpoint",
        input.challenge_id.as_str(),
        digest.as_str(),
    ] {
        assert!(!debug.contains(private));
    }
}

/// Claude quota status is not tool activity, but only a well-formed status
/// from the same session may be ignored while proving the pause boundary.
#[test]
fn claude_maintenance_accepts_same_session_allowed_quota_status() {
    let status = |value: &str| {
        json!({"type":"rate_limit_event","session_id":SESSION,
        "rate_limit_info":{"status":value}})
    };
    let mut observed = observer();
    observed.observe(&status("allowed"), None);
    observed.observe(&started(), None);
    observed.observe(&status("allowed_warning"), None);
    observed.observe(&receipt(), None);
    observed.observe(&result(), Some(true));
    assert_eq!(observed.assessment(), Assessment::Settled);
    observed.observe(&status("allowed"), None);
    assert_eq!(observed.assessment(), Assessment::Settled);

    let mut foreign = status("allowed");
    foreign["session_id"] = json!("foreign-session");
    let mut missing = status("allowed");
    missing.as_object_mut().unwrap().remove("session_id");
    for (frame, gap) in [
        (status("rejected"), Gap::NativeFailure),
        (status("unknown"), Gap::UnsupportedToolCoverage),
        (foreign, Gap::ForeignIdentity),
        (missing, Gap::MissingIdentity),
    ] {
        let mut observer = observer();
        observer.observe(&frame, None);
        observer.observe(&started(), None);
        observer.observe(&receipt(), None);
        observer.observe(&result(), Some(true));
        assert_eq!(observer.assessment(), Assessment::Unavailable(gap));
    }
}

/// New CLI versions emit lifecycle notices for runner-sent inputs, sometimes
/// before init or after the result. They cannot authorize unrelated activity.
#[test]
fn claude_maintenance_correlates_known_command_lifecycle() {
    let lifecycle = |command: &str, state: &str| {
        json!({"type":"command_lifecycle",
        "command_uuid":command,"state":state,"uuid":OTHER_INPUT,"session_id":SESSION})
    };
    let (input, digest) = request();
    let mut observed = Observer::new(ATTEMPT, false);
    observed.input_sent(INPUT);
    observed.observe(&lifecycle(INPUT, "queued"), None);
    observed.observe(
        &json!({"type":"system","subtype":"init","session_id":SESSION}),
        None,
    );
    observed.observe(&lifecycle(INPUT, "started"), None);
    observed.expect_checkpoint(ATTEMPT, SESSION, INPUT, &input.challenge_id, &digest);
    observed.observe(&started(), None);
    observed.observe(&receipt(), None);
    observed.observe(&result(), Some(true));
    observed.observe(&lifecycle(INPUT, "completed"), None);
    assert_eq!(observed.assessment(), Assessment::Settled);
    observed.observe(&lifecycle(INPUT, "queued"), None);
    assert_eq!(
        observed.assessment(),
        Assessment::Unavailable(Gap::InputCorrelation)
    );

    let mut foreign = lifecycle(INPUT, "started");
    foreign["session_id"] = json!("foreign-session");
    let mut scheduled = lifecycle(INPUT, "started");
    scheduled["wakeup_source"] = json!("schedule_wakeup");
    let mut malformed = lifecycle(INPUT, "queued");
    malformed.as_object_mut().unwrap().remove("uuid");
    for (frame, gap) in [
        (lifecycle(OTHER_INPUT, "queued"), Gap::InputCorrelation),
        (lifecycle(INPUT, "cancelled"), Gap::NativeFailure),
        (lifecycle(INPUT, "discarded"), Gap::NativeFailure),
        (lifecycle(INPUT, "refused"), Gap::NativeFailure),
        (lifecycle(INPUT, "other"), Gap::UnsupportedToolCoverage),
        (foreign, Gap::ForeignIdentity),
        (scheduled, Gap::AsyncCoverageUnavailable),
        (malformed, Gap::MissingIdentity),
    ] {
        let mut observer = observer();
        observer.observe(&frame, None);
        observer.observe(&started(), None);
        observer.observe(&receipt(), None);
        observer.observe(&result(), Some(true));
        assert_eq!(observer.assessment(), Assessment::Unavailable(gap));
    }
}

/// Some Claude versions re-emit init for each input. Only the same immutable
/// runtime identity at a new, previously settled input boundary is accepted.
#[test]
fn claude_maintenance_accepts_identical_init_for_a_new_input() {
    let init = json!({"type":"system","subtype":"init","session_id":SESSION,
        "model":"fixture","permissionMode":"default","tools":["fixture"]});
    let fresh = || {
        let mut observed = Observer::new(ATTEMPT, false);
        observed.input_sent(OTHER_INPUT);
        observed.observe(&init, None);
        let mut earlier = result();
        earlier["user_message_uuid"] = json!(OTHER_INPUT);
        observed.observe(&earlier, Some(true));
        observed.input_sent(INPUT);
        observed
    };
    let mut reinit = init.clone();
    reinit["uuid"] = json!(INPUT);
    reinit["user_message_uuids"] = json!([INPUT]);
    let mut observed = fresh();
    observed.observe(&reinit, None);
    let (input, digest) = request();
    observed.expect_checkpoint(ATTEMPT, SESSION, INPUT, &input.challenge_id, &digest);
    observed.observe(&started(), None);
    observed.observe(&receipt(), None);
    observed.observe(&result(), Some(true));
    assert_eq!(observed.assessment(), Assessment::Settled);

    let mut altered = reinit.clone();
    altered["permissionMode"] = json!("acceptEdits");
    let mut wrong_input = reinit.clone();
    wrong_input["user_message_uuids"] = json!([OTHER_INPUT]);
    for (frame, gap) in [
        (altered, Gap::ForeignIdentity),
        (wrong_input, Gap::InputCorrelation),
    ] {
        let mut observer = fresh();
        observer.observe(&frame, None);
        assert_eq!(observer.assessment(), Assessment::Unavailable(gap));
    }
    let mut duplicate = fresh();
    duplicate.observe(&reinit, None);
    duplicate.observe(&reinit, None);
    assert_eq!(
        duplicate.assessment(),
        Assessment::Unavailable(Gap::DuplicateOrUnstarted)
    );
}

/// A same-session request-status frame is informational during an active
/// turn, but cannot hide compaction, foreign work or activity after closure.
#[test]
fn claude_maintenance_accepts_active_model_request_status() {
    let status = json!({"type":"system","subtype":"status","status":"requesting",
        "uuid":OTHER_INPUT,"session_id":SESSION});
    let mut observed = observer();
    observed.observe(&status, None);
    let mut correlated = status.clone();
    correlated["user_message_uuid"] = json!(INPUT);
    correlated["user_message_uuids"] = json!([INPUT]);
    observed.observe(&correlated, None);
    let thinking = json!({"type":"system","subtype":"thinking_tokens", "uuid":OTHER_INPUT,
        "session_id":SESSION,"user_message_uuid":INPUT,"estimated_tokens":12,"estimated_tokens_delta":3});
    observed.observe(&thinking, None);
    observed.observe(&started(), None);
    observed.observe(&receipt(), None);
    observed.observe(&status, None);
    observed.observe(&result(), Some(true));
    assert_eq!(observed.assessment(), Assessment::Settled);
    observed.observe(&status, None);
    assert_eq!(
        observed.assessment(),
        Assessment::Unavailable(Gap::ActivityAfterCheckpoint)
    );

    let mut foreign = status.clone();
    foreign["session_id"] = json!("foreign-session");
    let mut missing = status.clone();
    missing.as_object_mut().unwrap().remove("uuid");
    let mut extra = status.clone();
    extra["permissionMode"] = json!("changed");
    let mut compacting = status.clone();
    compacting["status"] = json!("compacting");
    let mut foreign_input = status.clone();
    foreign_input["user_message_uuid"] = json!(OTHER_INPUT);
    let mut mixed_inputs = status.clone();
    mixed_inputs["user_message_uuids"] = json!([INPUT, OTHER_INPUT]);
    let mut invalid_thinking = thinking;
    invalid_thinking["estimated_tokens"] = json!(-1);
    for (frame, gap) in [
        (foreign, Gap::ForeignIdentity),
        (missing, Gap::MissingIdentity),
        (extra, Gap::AsyncCoverageUnavailable),
        (compacting, Gap::AsyncCoverageUnavailable),
        (foreign_input, Gap::InputCorrelation),
        (mixed_inputs, Gap::InputCorrelation),
        (invalid_thinking, Gap::AsyncCoverageUnavailable),
    ] {
        let mut observer = observer();
        observer.observe(&frame, None);
        assert_eq!(observer.assessment(), Assessment::Unavailable(gap));
    }
}

/// Native synthetic text may accompany one active turn, but cannot authorize
/// completion or hide a foreign, replayed, nested or additional input.
#[test]
fn claude_maintenance_accepts_bounded_synthetic_turn_companion() {
    let companion = json!({"type":"user","session_id":SESSION,"parent_tool_use_id":null,
        "isSynthetic":true,"uuid":OTHER_INPUT,"timestamp":"fixture",
        "message":{"role":"user","content":[{"type":"text","text":"native context"}]}});
    let mut observed = observer();
    observed.observe(&started(), None);
    observed.observe(&receipt(), None);
    let empty_start = json!({"type":"stream_event","session_id":SESSION,"parent_tool_use_id":null,
        "event":{"type":"message_start","message":{"id":"empty-message"}}});
    let empty_stop = json!({"type":"stream_event","session_id":SESSION,"parent_tool_use_id":null,
        "event":{"type":"message_stop"}});
    observed.observe(&empty_start, None);
    observed.observe(&empty_stop, None);
    observed.observe(&companion, None);
    assert_eq!(observed.assessment(), Assessment::AwaitingTurnCompletion);
    observed.observe(&result(), Some(true));
    assert_eq!(observed.assessment(), Assessment::Settled);
    observed.observe(&companion, None);
    assert_eq!(
        observed.assessment(),
        Assessment::Unavailable(Gap::ActivityAfterCheckpoint)
    );

    let mut unknown_origin = companion.clone();
    unknown_origin["origin"] = json!({"kind":"peer"});
    let mut foreign = companion.clone();
    foreign["session_id"] = json!("foreign-session");
    let mut same_input = companion.clone();
    same_input["uuid"] = json!(INPUT);
    let mut multiple = companion.clone();
    multiple["message"]["content"] = json!([{ "type":"text", "text":"one" },
        { "type":"text", "text":"two" }]);
    for (frame, gap) in [
        (unknown_origin, Gap::AsyncCoverageUnavailable),
        (foreign, Gap::ForeignIdentity),
        (same_input, Gap::InputCorrelation),
        (multiple, Gap::InputCorrelation),
    ] {
        let mut observer = observer();
        observer.observe(&frame, None);
        assert_eq!(observer.assessment(), Assessment::Unavailable(gap));
    }
    let mut duplicate = observer();
    duplicate.observe(&companion, None);
    duplicate.observe(&companion, None);
    assert_eq!(
        duplicate.assessment(),
        Assessment::Unavailable(Gap::InputCorrelation)
    );
    let mut late = observer();
    late.observe(&empty_start, None);
    late.observe(&empty_stop, None);
    late.observe(&json!({"type":"assistant","session_id":SESSION,"parent_tool_use_id":null,
        "message":{"id":"empty-message","role":"assistant","content":[{"type":"text","text":"late"}]}}), None);
    assert_eq!(
        late.assessment(),
        Assessment::Unavailable(Gap::DuplicateOrUnstarted)
    );
}

/// A frame delivered after the successful turn invalidates that foreground
/// observation; it must be drained under hold rather than used for cleanup.
#[test]
fn claude_maintenance_buffered_activity_invalidates_settlement() {
    let mut observed = observer();
    observed.observe(&started(), None);
    observed.observe(&receipt(), None);
    observed.observe(&result(), Some(true));
    assert_eq!(observed.assessment(), Assessment::Settled);
    observed.observe(&started(), None);
    assert_eq!(
        observed.assessment(),
        Assessment::Unavailable(Gap::ActivityAfterCheckpoint)
    );
}

/// A native digest alone is not broker acceptance; late exact binding works,
/// but a foreign attempt, session, input or digest can never be substituted.
#[test]
fn claude_maintenance_requires_exact_store_and_native_identity() {
    let mut unarmed = Observer::new(ATTEMPT, false);
    unarmed.input_sent(INPUT);
    unarmed.observe(
        &json!({"type":"system","subtype":"init","session_id":SESSION}),
        None,
    );
    unarmed.observe(&started(), None);
    unarmed.observe(&receipt(), None);
    unarmed.observe(&result(), Some(true));
    assert_eq!(unarmed.assessment(), Assessment::Unarmed);
    let (input, digest) = request();
    unarmed.expect_checkpoint(ATTEMPT, SESSION, INPUT, &input.challenge_id, &digest);
    assert_eq!(unarmed.assessment(), Assessment::Settled);
    for (attempt, session, uuid) in [
        ("foreign", SESSION, INPUT),
        (ATTEMPT, "foreign", INPUT),
        (ATTEMPT, SESSION, OTHER_INPUT),
    ] {
        let mut invalid = observer();
        invalid.expect_checkpoint(attempt, session, uuid, &input.challenge_id, &digest);
        assert!(matches!(invalid.assessment(), Assessment::Unavailable(_)));
    }
    let mut resumed = Observer::new(ATTEMPT, true);
    resumed.input_sent(INPUT);
    assert_eq!(
        resumed.assessment(),
        Assessment::Unavailable(Gap::InheritedWorkUnknown)
    );
}

/// A prior correlated input can finish before a fresh maintenance turn; native
/// input replay is accepted only once for its exact current UUID.
#[test]
fn claude_maintenance_tracks_each_runner_input_and_replay_once() {
    let mut first = Observer::new(ATTEMPT, false);
    first.input_sent(OTHER_INPUT);
    first.observe(
        &json!({"type":"system","subtype":"init","session_id":SESSION}),
        None,
    );
    let mut earlier = result();
    earlier["user_message_uuid"] = json!(OTHER_INPUT);
    first.observe(&earlier, Some(true));
    first.input_sent(INPUT);
    let (input, digest) = request();
    first.expect_checkpoint(ATTEMPT, SESSION, INPUT, &input.challenge_id, &digest);
    let replay = json!({"type":"user","session_id":SESSION,"parent_tool_use_id":null,"isReplay":true,"uuid":INPUT,"message":{"role":"user","content":"maintenance request"}});
    first.observe(&replay, None);
    first.observe(&started(), None);
    first.observe(&receipt(), None);
    first.observe(&result(), Some(true));
    assert_eq!(first.assessment(), Assessment::Settled);
    let mut duplicate = observer();
    duplicate.observe(&replay, None);
    duplicate.observe(&replay, None);
    assert_eq!(
        duplicate.assessment(),
        Assessment::Unavailable(Gap::InputCorrelation)
    );
    let mut wrong = observer();
    let mut replay = replay;
    replay["uuid"] = json!(OTHER_INPUT);
    wrong.observe(&replay, None);
    assert_eq!(
        wrong.assessment(),
        Assessment::Unavailable(Gap::InputCorrelation)
    );
    replay["uuid"] = json!(INPUT);
    replay["message"]["content"] = receipt()["message"]["content"].clone();
    let mut mixed = observer();
    mixed.observe(&replay, None);
    assert_eq!(
        mixed.assessment(),
        Assessment::Unavailable(Gap::InputCorrelation)
    );
    replay["message"]["content"] = json!("maintenance request");
    let mut late = observer();
    late.observe(&started(), None);
    late.observe(&replay, None);
    assert_eq!(
        late.assessment(),
        Assessment::Unavailable(Gap::InputCorrelation)
    );
}

/// Both partial-start + full-message and full-message-only native shapes are
/// supported; a duplicate start or incomplete partial input fails closed.
#[test]
fn claude_maintenance_streamed_start_requires_matching_full_call() {
    let event = |event: Value| json!({"type":"stream_event","session_id":SESSION,"parent_tool_use_id":null,"event":event});
    let message_start = event(json!({"type":"message_start","message":{"id":"message-1"}}));
    let tool_start = event(
        json!({"type":"content_block_start","index":0,"content_block":{
            "type":"tool_use","id":"pause-1","name":"mcp__agent_run_worker__maintenance_pause","input":{}
        }}),
    );
    let mut good = observer();
    good.observe(&message_start, None);
    good.observe(&tool_start, None);
    let encoded = serde_json::to_string(&request().0).unwrap();
    let split = encoded.len() / 2;
    for partial in [&encoded[..split], &encoded[split..]] {
        good.observe(&event(json!({"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":partial}})),None);
    }
    good.observe(&event(json!({"type":"content_block_stop","index":0})), None);
    good.observe(&event(json!({"type":"message_stop"})), None);
    good.observe(&started(), None);
    good.observe(&receipt(), None);
    good.observe(&result(), Some(true));
    assert_eq!(good.assessment(), Assessment::Settled);
    let mut duplicate = observer();
    duplicate.observe(&message_start, None);
    duplicate.observe(&tool_start, None);
    duplicate.observe(&tool_start, None);
    assert!(matches!(duplicate.assessment(), Assessment::Unavailable(_)));
    let mut partial = observer();
    partial.observe(&message_start, None);
    partial.observe(&tool_start, None);
    partial.observe(&receipt(), None);
    assert_eq!(
        partial.assessment(),
        Assessment::Unavailable(Gap::InvalidCheckpoint)
    );
    let mut mismatch = observer();
    mismatch.observe(&message_start, None);
    mismatch.observe(&tool_start, None);
    let mut different = request().0;
    different.checkpoint.push('!');
    mismatch.observe(&event(json!({"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":serde_json::to_string(&different).unwrap()}})),None);
    mismatch.observe(&event(json!({"type":"content_block_stop","index":0})), None);
    mismatch.observe(&event(json!({"type":"message_stop"})), None);
    mismatch.observe(&started(), None);
    assert_eq!(
        mismatch.assessment(),
        Assessment::Unavailable(Gap::InvalidCheckpoint)
    );
}

/// Claude may publish each complete assistant block before its stream stop;
/// correlate it to the open block without permitting duplicate or foreign work.
#[test]
fn claude_maintenance_accepts_interleaved_complete_stream_blocks() {
    let event = |event: Value| {
        json!({"type":"stream_event","session_id":SESSION,
        "parent_tool_use_id":null,"event":event})
    };
    let fragment = |id: &str, block: Value| {
        json!({"type":"assistant","session_id":SESSION,
        "parent_tool_use_id":null,"message":{"id":id,"role":"assistant","content":[block]}})
    };
    let start = event(json!({"type":"message_start","message":{"id":"fragmented"}}));
    let thinking =
        event(json!({"type":"content_block_start","index":0,"content_block":{"type":"thinking"}}));
    let block = fragment(
        "fragmented",
        json!({"type":"thinking","thinking":"private"}),
    );
    let stop = event(json!({"type":"content_block_stop","index":0}));
    let mut observed = observer();
    observed.observe(&start, None);
    observed.observe(&thinking, None);
    observed.observe(&block, None);
    observed.observe(&stop, None);
    observed.observe(
        &event(json!({"type":"content_block_start","index":1,
        "content_block":{"type":"text","text":""}})),
        None,
    );
    observed.observe(
        &fragment("fragmented", json!({"type":"text","text":"working"})),
        None,
    );
    observed.observe(&event(json!({"type":"content_block_stop","index":1})), None);
    observed.observe(&event(json!({"type":"message_stop"})), None);
    observed.observe(&started(), None);
    observed.observe(&receipt(), None);
    observed.observe(&result(), Some(true));
    assert_eq!(observed.assessment(), Assessment::Settled);

    let mut repeated = observer();
    repeated.observe(&start, None);
    repeated.observe(&thinking, None);
    repeated.observe(&block, None);
    repeated.observe(&block, None);
    assert_eq!(
        repeated.assessment(),
        Assessment::Unavailable(Gap::DuplicateOrUnstarted)
    );
    let mut different = observer();
    different.observe(&start, None);
    different.observe(&thinking, None);
    different.observe(
        &fragment("fragmented", json!({"type":"text","text":"foreign"})),
        None,
    );
    assert_eq!(
        different.assessment(),
        Assessment::Unavailable(Gap::DuplicateOrUnstarted)
    );

    let mut tool = observer();
    tool.observe(
        &event(json!({"type":"message_start","message":{"id":"message-1"}})),
        None,
    );
    tool.observe(&event(json!({"type":"content_block_start","index":0,"content_block":{
        "type":"tool_use","id":"pause-1","name":"mcp__agent_run_worker__maintenance_pause","input":{}}})), None);
    tool.observe(
        &event(json!({"type":"content_block_delta","index":0,"delta":{
        "type":"input_json_delta","partial_json":serde_json::to_string(&request().0).unwrap()}})),
        None,
    );
    tool.observe(&started(), None);
    tool.observe(&event(json!({"type":"content_block_stop","index":0})), None);
    tool.observe(&event(json!({"type":"message_stop"})), None);
    tool.observe(&receipt(), None);
    tool.observe(&result(), Some(true));
    assert_eq!(tool.assessment(), Assessment::Settled);
}

/// A native stream cannot fill a missing or repeated block/message boundary
/// with a later valid assistant message or callback.
#[test]
fn claude_maintenance_rejects_unstarted_duplicate_or_open_stream_blocks() {
    let event = |event: Value| json!({"type":"stream_event","session_id":SESSION,"parent_tool_use_id":null,"event":event});
    let message = event(json!({"type":"message_start","message":{"id":"message-1"}}));
    let text = event(
        json!({"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}),
    );
    let stop = event(json!({"type":"content_block_stop","index":0}));
    let finish_message = event(json!({"type":"message_stop"}));
    let delta = event(
        json!({"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"x"}}),
    );
    for events in [
        vec![text.clone()],
        vec![message.clone(), delta],
        vec![message.clone(), stop.clone()],
        vec![message.clone(), text.clone(), text.clone()],
        vec![message.clone(), text.clone(), stop.clone(), text.clone()],
        vec![message.clone(), text.clone(), finish_message.clone()],
        vec![
            message.clone(),
            finish_message.clone(),
            finish_message.clone(),
        ],
        vec![message.clone(), text.clone(), stop.clone(), started()],
    ] {
        let mut observer = observer();
        for frame in events {
            observer.observe(&frame, None);
        }
        observer.observe(&started(), None);
        observer.observe(&receipt(), None);
        observer.observe(&result(), Some(true));
        assert!(matches!(observer.assessment(), Assessment::Unavailable(_)));
    }
}

/// Missing/foreign/duplicate tool ids, ambiguous receipts and incorrect result
/// correlation permanently block any later successful-looking sequence.
#[test]
fn claude_maintenance_fails_closed_for_ambiguous_identity_and_order() {
    let mut missing = started();
    missing["message"]["content"][0]
        .as_object_mut()
        .unwrap()
        .remove("id");
    let mut missing_message = started();
    missing_message["message"]
        .as_object_mut()
        .unwrap()
        .remove("id");
    let mut missing_parent = started();
    missing_parent
        .as_object_mut()
        .unwrap()
        .remove("parent_tool_use_id");
    let mut nested_parent = started();
    nested_parent["parent_tool_use_id"] = json!("foreign-parent");
    let mut foreign_session = receipt();
    foreign_session["session_id"] = json!("foreign");
    let mut foreign_tool = receipt();
    foreign_tool["message"]["content"][0]["tool_use_id"] = json!("other");
    let mut bad_receipt = receipt();
    bad_receipt["message"]["content"][0]["content"] =
        json!("receipt is aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
    let mut wrong_digest = receipt();
    wrong_digest["message"]["content"][0]["content"] =
        json!(json!({"sha256":"c".repeat(64),"duplicate":false}).to_string());
    let mut extra_receipt_content = receipt();
    extra_receipt_content["message"]["content"][0]["content"] = json!([
        {"type":"text","text":json!({"sha256":request().1,"duplicate":false}).to_string()},
        {"type":"text","text":"extra"}
    ]);
    let mut native_error = receipt();
    native_error["message"]["content"][0]["is_error"] = json!(true);
    let mut wrong_uuid = result();
    wrong_uuid["user_message_uuid"] = json!(OTHER_INPUT);
    let mut missing_uuid = result();
    missing_uuid
        .as_object_mut()
        .unwrap()
        .remove("user_message_uuid");
    let mut missing_session = receipt();
    missing_session
        .as_object_mut()
        .unwrap()
        .remove("session_id");
    let mut bad_status = result();
    bad_status["is_error"] = json!(true);
    let mut missing_status = result();
    missing_status.as_object_mut().unwrap().remove("is_error");
    for (events, scope) in [
        (vec![missing], None),
        (vec![missing_message], None),
        (vec![missing_parent], None),
        (vec![nested_parent], None),
        (vec![receipt()], None),
        (vec![started(), started()], None),
        (vec![started(), foreign_session], None),
        (vec![started(), foreign_tool], None),
        (vec![started(), bad_receipt], None),
        (vec![started(), wrong_digest], None),
        (vec![started(), extra_receipt_content], None),
        (vec![started(), receipt(), receipt()], None),
        (vec![started(), native_error], None),
        (vec![started(), missing_session], None),
        (vec![started(), receipt(), wrong_uuid], Some(true)),
        (vec![started(), receipt(), missing_uuid], Some(true)),
        (vec![started(), receipt(), bad_status], Some(true)),
        (vec![started(), receipt(), missing_status], Some(true)),
        (vec![started(), receipt(), result()], Some(false)),
        (vec![result()], Some(true)),
    ] {
        let mut observer = observer();
        for frame in events {
            let is_result = frame["type"] == "result";
            observer.observe(&frame, if is_result { scope } else { None });
        }
        observer.observe(&started(), None);
        observer.observe(&receipt(), None);
        observer.observe(&result(), Some(true));
        assert!(matches!(observer.assessment(), Assessment::Unavailable(_)));
    }
}

/// Generic tools and native background notifications remain unknown even if
/// later frames claim completion; no quiescence is inferred from silence.
#[test]
fn claude_maintenance_rejects_generic_tools_and_background_activity() {
    let mut generic = started();
    generic["message"]["content"][0]["name"] = json!("Bash");
    let background = json!({"type":"user","session_id":SESSION,"parent_tool_use_id":null,"origin":{"kind":"task-notification"},"message":{"role":"user","content":"done"}});
    let missing_delta = json!({"type":"stream_event","session_id":SESSION,"parent_tool_use_id":null,"event":{"type":"content_block_delta","index":9,"delta":{"type":"input_json_delta","partial_json":"{}"}}});
    let task_start = json!({"type":"system","session_id":SESSION,"subtype":"task_started"});
    for frame in [generic, background, missing_delta, task_start] {
        let mut observer = observer();
        observer.observe(&frame, None);
        observer.observe(&started(), None);
        observer.observe(&receipt(), None);
        observer.observe(&result(), Some(true));
        assert!(matches!(observer.assessment(), Assessment::Unavailable(_)));
    }
    let mut completed = observer();
    completed.observe(&started(), None);
    completed.observe(&receipt(), None);
    completed.observe(&result(), Some(true));
    completed.input_sent(OTHER_INPUT);
    assert_eq!(
        completed.assessment(),
        Assessment::Unavailable(Gap::ActivityAfterCheckpoint)
    );
}

/// Completed built-in foreground tools, including parallel file operations,
/// preserve coverage only when every exact native result has arrived.
#[test]
fn claude_maintenance_accepts_completed_foreground_tools() {
    let call = |id: &str, name: &str, input: Value| json!({"type":"tool_use","id":id,"name":name,"input":input});
    let assistant = |id: &str, blocks: Vec<Value>| {
        json!({"type":"assistant","session_id":SESSION,
        "parent_tool_use_id":null,"message":{"id":id,"role":"assistant","content":blocks}})
    };
    let reply = |ids: &[&str]| {
        json!({"type":"user","session_id":SESSION,"parent_tool_use_id":null,
        "message":{"role":"user","content":ids.iter().map(|id|json!({"type":"tool_result","tool_use_id":id,"content":"ok"})).collect::<Vec<_>>()}})
    };
    let mut observed = foreground_observer();
    observed.observe(
        &assistant(
            "read-message",
            vec![
                call("read-1", "Read", json!({"file_path":"file"})),
                call("read-2", "Grep", json!({"pattern":"x"})),
            ],
        ),
        None,
    );
    observed.observe(&reply(&["read-2", "read-1"]), None);
    for (index, name) in ["Glob", "Edit", "Write", "NotebookEdit"]
        .into_iter()
        .enumerate()
    {
        let id = format!("tool-{index}");
        observed.observe(
            &assistant(
                &format!("foreground-message-{index}"),
                vec![call(&id, name, json!({"file_path":"file"}))],
            ),
            None,
        );
        observed.observe(&reply(&[&id]), None);
    }
    observed.observe(
        &assistant(
            "bash-message",
            vec![call(
                "bash-1",
                "Bash",
                json!({"command":"true","run_in_background":false}),
            )],
        ),
        None,
    );
    let mut bash = reply(&["bash-1"]);
    bash["tool_use_result"] = json!({"stdout":"","stderr":"","interrupted":false});
    observed.observe(&bash, None);
    let (input, digest) = request();
    observed.expect_checkpoint(ATTEMPT, SESSION, INPUT, &input.challenge_id, &digest);
    observed.observe(&started(), None);
    observed.observe(&receipt(), None);
    observed.observe(&result(), Some(true));
    assert_eq!(observed.assessment(), Assessment::Settled);
}

/// A streamed foreground call must match its full native input before its
/// result can release the call; divergent or incomplete input fails closed.
#[test]
fn claude_maintenance_correlates_streamed_foreground_input() {
    let event = |event: Value| {
        json!({"type":"stream_event","session_id":SESSION,
        "parent_tool_use_id":null,"event":event})
    };
    let start = event(json!({"type":"message_start","message":{"id":"stream-read"}}));
    let block = event(
        json!({"type":"content_block_start","index":0,"content_block":{
        "type":"tool_use","id":"read-stream","name":"Read","input":{}}}),
    );
    let delta = |text: &str| {
        event(json!({"type":"content_block_delta","index":0,
        "delta":{"type":"input_json_delta","partial_json":text}}))
    };
    let full = json!({"type":"assistant","session_id":SESSION,"parent_tool_use_id":null,
        "message":{"id":"stream-read","role":"assistant","content":[{"type":"tool_use",
        "id":"read-stream","name":"Read","input":{"file_path":"file"}}]}});
    let reply = json!({"type":"user","session_id":SESSION,"parent_tool_use_id":null,
        "message":{"role":"user","content":[{"type":"tool_result","tool_use_id":"read-stream","content":"ok"}]}});
    let mut observed = foreground_observer();
    observed.observe(&start, None);
    observed.observe(&block, None);
    observed.observe(&delta("{\"file_path\":\"file\"}"), None);
    observed.observe(&full, None);
    observed.observe(&event(json!({"type":"content_block_stop","index":0})), None);
    observed.observe(&event(json!({"type":"message_stop"})), None);
    observed.observe(&reply, None);
    let (input, digest) = request();
    observed.expect_checkpoint(ATTEMPT, SESSION, INPUT, &input.challenge_id, &digest);
    observed.observe(&started(), None);
    observed.observe(&receipt(), None);
    observed.observe(&result(), Some(true));
    assert_eq!(observed.assessment(), Assessment::Settled);

    let mut different = foreground_observer();
    different.observe(&start, None);
    different.observe(&block, None);
    different.observe(&delta("{\"file_path\":\"other\"}"), None);
    different.observe(&full, None);
    assert_eq!(
        different.assessment(),
        Assessment::Unavailable(Gap::InputCorrelation)
    );

    let mut late = foreground_observer();
    late.observe(&start, None);
    late.observe(&block, None);
    late.observe(&delta("{\"file_path\":\"file\"}"), None);
    late.observe(&full, None);
    late.observe(&delta(" "), None);
    assert_eq!(
        late.assessment(),
        Assessment::Unavailable(Gap::InputCorrelation)
    );

    let mut missing = foreground_observer();
    missing.observe(&start, None);
    missing.observe(&block, None);
    missing.observe(&reply, None);
    assert!(matches!(missing.assessment(), Assessment::Unavailable(_)));
}

/// Stream block indexes are reused in later assistant messages even while
/// earlier parallel calls are still waiting for their native results.
#[test]
fn claude_maintenance_tracks_parallel_streams_with_reused_indexes() {
    let event = |event: Value| {
        json!({"type":"stream_event","session_id":SESSION,
        "parent_tool_use_id":null,"event":event})
    };
    let mut observed = foreground_observer();
    for (message, id, name) in [("one", "tool-one", "Read"), ("two", "tool-two", "Grep")] {
        observed.observe(
            &event(json!({"type":"message_start","message":{"id":message}})),
            None,
        );
        observed.observe(
            &event(json!({"type":"content_block_start","index":0,
            "content_block":{"type":"tool_use","id":id,"name":name,"input":{}}})),
            None,
        );
        observed.observe(
            &event(json!({"type":"content_block_delta","index":0,
            "delta":{"type":"input_json_delta","partial_json":"{}"}})),
            None,
        );
        observed.observe(&json!({"type":"assistant","session_id":SESSION,"parent_tool_use_id":null,
            "message":{"id":message,"role":"assistant","content":[{"type":"tool_use","id":id,"name":name,"input":{}}]}}),None);
        observed.observe(&event(json!({"type":"content_block_stop","index":0})), None);
        observed.observe(&event(json!({"type":"message_stop"})), None);
    }
    observed.observe(&json!({"type":"user","session_id":SESSION,"parent_tool_use_id":null,
        "message":{"role":"user","content":[{"type":"tool_result","tool_use_id":"tool-one","content":"ok"},
        {"type":"tool_result","tool_use_id":"tool-two","content":"ok"}]}}),None);
    let (input, digest) = request();
    observed.expect_checkpoint(ATTEMPT, SESSION, INPUT, &input.challenge_id, &digest);
    observed.observe(&started(), None);
    observed.observe(&receipt(), None);
    observed.observe(&result(), Some(true));
    assert_eq!(observed.assessment(), Assessment::Settled);
}

/// Background, interrupted, uncorrelated and still-running tools never become
/// evidence that a maintenance pause is safe.
#[test]
fn claude_maintenance_refuses_unsettled_foreground_tools() {
    let call = |name: &str, input: Value| {
        json!({"type":"assistant","session_id":SESSION,
        "parent_tool_use_id":null,"message":{"id":"tool-message","role":"assistant","content":[
        {"type":"tool_use","id":"tool-1","name":name,"input":input}]}})
    };
    let reply = |output: Value| {
        json!({"type":"user","session_id":SESSION,"parent_tool_use_id":null,
        "tool_use_result":output,"message":{"role":"user","content":[{"type":"tool_result","tool_use_id":"tool-1","content":"ok"}]}})
    };
    for frame in [
        call(
            "Bash",
            json!({"command":"sleep 1","run_in_background":true}),
        ),
        call(
            "Bash",
            json!({"command":"sleep 1","run_in_background":null}),
        ),
        call("Agent", json!({"prompt":"work"})),
        call("mcp__external__tool", json!({})),
    ] {
        let mut observer = foreground_observer();
        observer.observe(&frame, None);
        assert!(matches!(observer.assessment(), Assessment::Unavailable(_)));
    }
    for output in [
        json!({"stdout":"","stderr":"","interrupted":false,"backgroundTaskId":"task"}),
        json!({"stdout":"","stderr":"","interrupted":false,"backgroundTaskId":null}),
        json!({"stdout":"","stderr":"","interrupted":true}),
        json!({"stdout":"","stderr":"","interrupted":false,"timedOutAfterMs":1}),
        json!({"stdout":"","stderr":"","interrupted":false,"unrecognized":true}),
        json!({"stdout":"","stderr":""}),
        Value::Null,
    ] {
        let mut observer = foreground_observer();
        observer.observe(&call("Bash", json!({"command":"true"})), None);
        observer.observe(&reply(output), None);
        assert!(matches!(observer.assessment(), Assessment::Unavailable(_)));
    }
    let mut stripped = foreground_observer();
    stripped.observe(&call("Bash", json!({"command":"true"})), None);
    let mut background_note = reply(json!({"stdout":"","stderr":"","interrupted":false}));
    background_note["message"]["content"][0]["content"] = json!("Command moved to the background");
    stripped.observe(&background_note, None);
    assert_eq!(
        stripped.assessment(),
        Assessment::Unavailable(Gap::AsyncCoverageUnavailable)
    );
    let mut pending = foreground_observer();
    pending.observe(&call("Read", json!({"file_path":"file"})), None);
    pending.observe(&started(), None);
    assert!(matches!(pending.assessment(), Assessment::Unavailable(_)));
}
