//! Strict initial-task request and response wire contracts; no database involved.
use agent_run_domain::initial_task::{
    InitialTaskCapability, InitialTaskMode, InitialTaskReason, InitialTaskRequest,
    InitialTaskResponse, MAX_PAGE_BYTES, MAX_TASK_BYTES,
};
use serde_json::{Value, json};

const STORE: &str = "store_00000000000000000000000000000000";
const AGENT: &str = "ag-20260826-120000-0123456789";
const PIN: &str = "pin_00000000000040008000000000000000";

fn reference() -> String {
    format!("ex1_{}", "0".repeat(64))
}

fn strict() -> Value {
    json!({"version":1,"mode":"strict","expected_store_id":STORE,"agent_id":AGENT,"sequence":1,
        "execution_ref":reference(),"pin_id":PIN})
}

fn current() -> Value {
    json!({"version":1,"mode":"current","agent_id":AGENT})
}

fn parse(value: &Value) -> Option<InitialTaskRequest> {
    serde_json::from_value::<InitialTaskRequest>(value.clone())
        .ok()
        .filter(|request| request.validate().is_ok())
}

/// The registry schema and the Rust decoder accept and reject the same requests.
#[test]
fn schema_and_decoder_agree_on_modes_and_fences() {
    let tool = agent_run_domain::tool("initial_task").unwrap();
    assert!(tool.annotations.read_only_hint && !tool.annotations.destructive_hint);
    assert!(tool.legacy_for.is_none());
    let schema = jsonschema::validator_for(&tool.input_schema).unwrap();
    let digest = "a".repeat(64);
    let mut cases: Vec<(Value, bool)> = vec![(strict(), true), (current(), true)];
    let mut add = |mut value: Value, edits: &[(&str, Value)], ok: bool| {
        for (key, edit) in edits {
            if edit.is_null() && *key != "$null" {
                value.as_object_mut().unwrap().remove(*key);
            } else {
                value[*key] = edit.clone();
            }
        }
        cases.push((value, ok));
    };
    // Strict needs every fence; current rejects a pin and accepts an optional ref.
    for key in ["expected_store_id", "sequence", "execution_ref", "pin_id"] {
        add(strict(), &[(key, Value::Null)], false);
    }
    add(current(), &[("pin_id", json!(PIN))], false);
    add(current(), &[("execution_ref", json!(reference()))], true);
    add(current(), &[("expected_store_id", json!(STORE))], true);
    add(current(), &[("sequence", json!(1))], true);
    add(current(), &[("sequence", json!(0))], false);
    // Unknown mode/field, wrong version and string-typed numbers.
    add(current(), &[("mode", json!("latest"))], false);
    add(current(), &[("surprise", json!(true))], false);
    add(current(), &[("version", json!(2))], false);
    add(current(), &[("offset", json!("0"))], false);
    // Continuations: a nonzero offset needs both fences; fences are only paired.
    add(current(), &[("offset", json!(1))], false);
    add(
        current(),
        &[("offset", json!(1)), ("expected_sha256", json!(digest))],
        false,
    );
    add(
        current(),
        &[("offset", json!(1)), ("expected_total_bytes", json!(10))],
        false,
    );
    add(
        current(),
        &[
            ("offset", json!(1)),
            ("expected_sha256", json!(digest)),
            ("expected_total_bytes", json!(10)),
        ],
        true,
    );
    add(
        current(),
        &[
            ("expected_sha256", json!(digest)),
            ("expected_total_bytes", json!(10)),
        ],
        true,
    );
    add(current(), &[("expected_sha256", json!(digest))], false);
    add(
        current(),
        &[
            ("expected_sha256", json!(digest)),
            ("expected_total_bytes", json!(MAX_TASK_BYTES + 1)),
        ],
        false,
    );
    add(
        current(),
        &[
            ("expected_sha256", json!("A".repeat(64))),
            ("expected_total_bytes", json!(1)),
        ],
        false,
    );
    // Page budget bounds: at least one UTF-8 scalar, at most the compiled page.
    add(current(), &[("max_bytes", json!(3))], false);
    add(current(), &[("max_bytes", json!(4))], true);
    add(current(), &[("max_bytes", json!(MAX_PAGE_BYTES))], true);
    add(
        current(),
        &[("max_bytes", json!(MAX_PAGE_BYTES + 1))],
        false,
    );
    add(current(), &[("max_bytes", json!(-1))], false);
    for (value, ok) in &cases {
        assert_eq!(schema.is_valid(value), *ok, "schema disagrees for {value}");
        assert_eq!(parse(value).is_some(), *ok, "decoder disagrees for {value}");
    }
    // An offset at or past the fenced length cannot be expressed in JSON Schema, only checked.
    let mut beyond = current();
    beyond["offset"] = json!(10);
    beyond["expected_sha256"] = json!(digest);
    beyond["expected_total_bytes"] = json!(10);
    assert!(parse(&beyond).is_none());
    // Explicit null is never a fence or an omitted option.
    for key in [
        "expected_store_id",
        "sequence",
        "execution_ref",
        "expected_sha256",
        "expected_total_bytes",
    ] {
        let mut bad = current();
        bad[key] = Value::Null;
        assert!(
            serde_json::from_value::<InitialTaskRequest>(bad).is_err(),
            "{key}"
        );
    }
    let omitted = parse(&current()).unwrap();
    assert_eq!(
        (omitted.offset, omitted.max_bytes, omitted.sequence()),
        (0, MAX_PAGE_BYTES, 1)
    );
}

/// Discovery is opt-in, typed and strict.
#[test]
fn capability_flag_is_strict_and_typed() {
    use agent_run_domain::observation::CapabilitiesQuery;
    assert!(
        !serde_json::from_value::<CapabilitiesQuery>(json!({}))
            .unwrap()
            .include_initial_task
    );
    assert!(
        serde_json::from_value::<CapabilitiesQuery>(json!({"include_initial_task":true}))
            .unwrap()
            .include_initial_task
    );
    let capability = serde_json::to_value(InitialTaskCapability::current()).unwrap();
    assert_eq!(
        capability,
        json!({"version":1,"method":"initial_task","modes":["strict","current"],
            "max_task_bytes":524288,"max_page_bytes":65536})
    );
    let schema = agent_run_domain::tool("capabilities").unwrap();
    assert!(schema.input_schema["properties"]["include_initial_task"].is_object());
    let output = jsonschema::validator_for(schema.output_schema.as_ref().unwrap()).unwrap();
    assert!(output.is_valid(&json!({
        "schema_version":1,"product":"agent-run","version":"1","source_revision":null,
        "transports":["cli"],"limits":{"socket_max_frame_bytes":1048576,"answer_max_bytes":16777216,
        "answer_chunk_default_bytes":65536,"answer_chunk_max_bytes":131072},
        "read_methods":[],"initial_task":capability})));
}

fn available(request: &Value) -> Value {
    let mut value = json!({"state":"available","version":1,"mode":request["mode"],
        "store_id":STORE,"agent_id":AGENT,"sequence":1,"execution_ref":reference(),
        "total_bytes":10,"sha256":"b".repeat(64),"offset":0,"end":4,"next_offset":4,
        "text":"abcd","complete":false});
    if request["mode"] == "strict" {
        value["pin_id"] = json!(PIN);
    }
    value
}

fn validate(request: &Value, response: &Value) -> bool {
    let request = serde_json::from_value::<InitialTaskRequest>(request.clone()).unwrap();
    serde_json::from_value::<InitialTaskResponse>(response.clone())
        .is_ok_and(|response| response.validate_for(&request).is_ok())
}

/// Response arithmetic, correlation and mode-specific claims are checked before publication.
#[test]
fn response_validation_rejects_inconsistent_pages_and_claims() {
    let tool = agent_run_domain::tool("initial_task").unwrap();
    let output = jsonschema::validator_for(tool.output_schema.as_ref().unwrap()).unwrap();
    for request in [strict(), current()] {
        let good = available(&request);
        assert!(validate(&request, &good));
        assert!(output.is_valid(&good));
        let mut last = good.clone();
        last["offset"] = json!(0);
        last["end"] = json!(10);
        last["total_bytes"] = json!(10);
        last["text"] = json!("0123456789");
        last["next_offset"] = Value::Null;
        last["complete"] = json!(true);
        assert!(validate(&request, &last));
        assert!(output.is_valid(&last));
        for (key, value) in [
            ("state", json!("unavailable")),
            ("agent_id", json!("ag-20260826-120000-9999999999")),
            ("sequence", json!(2)),
            ("offset", json!(1)),
            ("end", json!(5)),
            ("text", json!("abc")),
            ("complete", json!(true)),
            ("next_offset", Value::Null),
            ("next_offset", json!(3)),
            ("total_bytes", json!(4)),
            ("total_bytes", json!(MAX_TASK_BYTES + 1)),
            (
                "mode",
                json!(if request["mode"] == "strict" {
                    "current"
                } else {
                    "strict"
                }),
            ),
            ("sha256", json!("short")),
        ] {
            let mut bad = good.clone();
            bad[key] = value;
            assert!(!validate(&request, &bad), "{key} accepted");
        }
        // Only a client-supplied store fence can contradict the observed identity.
        let mut foreign = good.clone();
        foreign["store_id"] = json!("store_ffffffffffffffffffffffffffffffff");
        assert_eq!(validate(&request, &foreign), request["mode"] == "current");
        // Likewise the ref is a fence only when the client supplied one (always, in strict).
        let mut other_ref = good.clone();
        other_ref["execution_ref"] = json!(format!("ex1_{}", "1".repeat(64)));
        assert_eq!(validate(&request, &other_ref), request["mode"] == "current");
        let mut missing = good.clone();
        missing.as_object_mut().unwrap().remove("next_offset");
        assert!(!validate(&request, &missing), "next_offset must be present");
        let mut extra = good.clone();
        extra["extra"] = json!(1);
        assert!(!validate(&request, &extra));
    }
    // Strict pages echo the pin, current pages never claim one.
    let mut pinless = available(&strict());
    pinless.as_object_mut().unwrap().remove("pin_id");
    assert!(!validate(&strict(), &pinless));
    let mut claimed = available(&current());
    claimed["pin_id"] = json!(PIN);
    assert!(!validate(&current(), &claimed));
    // Page bytes never exceed the request budget, and fences must match the page.
    let mut small = current();
    small["max_bytes"] = json!(4);
    let mut long = available(&small);
    long["end"] = json!(5);
    long["text"] = json!("abcde");
    assert!(!validate(&small, &long));
    let mut fenced = current();
    fenced["offset"] = json!(4);
    fenced["expected_sha256"] = json!("b".repeat(64));
    fenced["expected_total_bytes"] = json!(10);
    let mut next = available(&fenced);
    next["offset"] = json!(4);
    next["end"] = json!(8);
    next["next_offset"] = json!(8);
    assert!(validate(&fenced, &next));
    next["sha256"] = json!("c".repeat(64));
    assert!(!validate(&fenced, &next));
}

/// Unavailable results carry only closed metadata and honour mode-specific reasons.
#[test]
fn unavailable_reasons_are_closed_and_mode_scoped() {
    let tool = agent_run_domain::tool("initial_task").unwrap();
    let output = jsonschema::validator_for(tool.output_schema.as_ref().unwrap()).unwrap();
    let reasons = [
        "not_found",
        "retention_deleted",
        "not_root_execution",
        "execution_ref_unavailable",
        "execution_ref_mismatch",
        "pin_missing",
        "pin_released",
        "pin_mismatch",
        "coverage_incomplete",
        "task_missing",
        "task_mismatch",
        "task_too_large",
        "provider_identity_invalid",
        "content_changed",
    ];
    let typed: Vec<InitialTaskReason> = reasons
        .iter()
        .map(|reason| serde_json::from_value(json!(reason)).unwrap())
        .collect();
    assert_eq!(typed.len(), reasons.len());
    assert!(serde_json::from_value::<InitialTaskReason>(json!("because")).is_err());
    let fenced = |mut request: Value| {
        request["expected_sha256"] = json!("b".repeat(64));
        request["expected_total_bytes"] = json!(10);
        request["execution_ref"] = json!(reference());
        request
    };
    for mode in [InitialTaskMode::Strict, InitialTaskMode::Current] {
        let request = fenced(if mode == InitialTaskMode::Strict {
            strict()
        } else {
            current()
        });
        for reason in reasons {
            let response = json!({"state":"unavailable","version":1,"mode":request["mode"],
                "store_id":STORE,"agent_id":AGENT,"reason":reason});
            assert!(output.is_valid(&response));
            let strict_only = matches!(
                reason,
                "retention_deleted"
                    | "pin_missing"
                    | "pin_released"
                    | "pin_mismatch"
                    | "coverage_incomplete"
            );
            assert_eq!(
                validate(&request, &response),
                mode == InitialTaskMode::Strict || !strict_only,
                "{reason} in {mode:?}"
            );
        }
        // No text rides along with a refusal.
        let with_text = json!({"state":"unavailable","version":1,"mode":request["mode"],
            "store_id":STORE,"agent_id":AGENT,"reason":"task_mismatch","text":"leak"});
        assert!(serde_json::from_value::<InitialTaskResponse>(with_text.clone()).is_err());
        assert!(!output.is_valid(&with_text));
    }
    // Reasons that need a client fence cannot answer an unfenced request.
    let plain = current();
    for reason in ["execution_ref_mismatch", "content_changed"] {
        let response = json!({"state":"unavailable","version":1,"mode":"current",
            "store_id":STORE,"agent_id":AGENT,"reason":reason});
        assert!(!validate(&plain, &response), "{reason}");
    }
}
